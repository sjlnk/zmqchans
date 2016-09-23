(ns zmqchans.core
  (:use
   [zmqchans.utils])
  (:require
   [clojure.string :refer [split join]]
   [clojure.core.match :refer [match]]
   [clojure.set :refer [map-invert]]
   [clojure.core.async
    :refer [chan  close! go thread <! >! <!! >!! alts!! timeout offer!
            promise-chan]])
  (:import
   [org.zeromq ZMQ ZMQ$Poller ZMQ$Socket ZMQException]
   ))

;; ZMQ_STREAM socket is not supported by jzmq as of v3.1
(def ^:const socket-types
  {:pair   ZMQ/PAIR
   :pub    ZMQ/PUB
   :sub    ZMQ/SUB
   :req    ZMQ/REQ
   :rep    ZMQ/REP
   :dealer ZMQ/DEALER
   :router ZMQ/ROUTER
   :xpub   ZMQ/XPUB
   :xsub   ZMQ/XSUB
   :pull   ZMQ/PULL
   :push   ZMQ/PUSH
   })

;; Prevent overlapping lines printed from multiple threads.
(def ^:private print-synchronizer (Object.))

(def ^:dynamic *print-stderr* (not (System/getProperty "zmqchans.quiet")))

(defmacro error [fmt & args]
  (when *print-stderr*
    `(locking print-synchronizer
       (binding [*out* *err*] (println (format ~fmt ~@args))))))

(def ^:dynamic *print-trace* (System/getProperty "zmqchans.trace"))
;;(def ^:dynamic *print-trace* true)

(defmacro trace [fmt & args]
  (when *print-trace*
    `(locking print-synchronizer (println (format ~fmt ~@args)))))

(defn- send!
  [^ZMQ$Socket sock msg]
  (let [msg (if (coll? msg) msg [msg])]
    (loop [[head & tail] msg]
      ;; Investigate whether error catching here is a good idea or whether
      ;; the thread should be allowed to crash in an erroneous condition.
      (let [res (try (.send sock head (if tail
                                        (bit-or ZMQ/NOBLOCK ZMQ/SNDMORE)
                                        ZMQ/NOBLOCK))
                     (catch ZMQException e e))
            ]
        (cond
          (instance? Exception res)
          (error "*ERROR* message not sent on %s: %s" sock res)
          (= false res)
          (let [sock-type (str (get (map-invert socket-types)
                                    (try (.getType sock)
                                         (catch ZMQException e nil))))]
            (error "*ERROR* message not sent on %s (%s)" sock sock-type))
          tail (recur tail))))))

(defn- receive-all
  "Receive all data parts from the socket, returning a vector of byte arrays.
  If the socket does not contain a multipart message, returns a plain byte array."
  [^ZMQ$Socket sock]
  (loop [acc (transient [])]
    (let [new-acc (conj! acc (.recv sock))]
      (if (.hasReceiveMore sock)
        (recur new-acc)
        (let [res (persistent! new-acc)]
          (if (= 1 (count res))
            (first res) res))))))

(defn- poll
  "Blocking poll that returns a [val, socket] tuple.
  If multiple sockets are ready, one is chosen to be read from nondeterministically."
  [socks]
  ;; TODO: what's the perf cost of creating a new poller all the time?
  (let [n      (count socks)
        poller (ZMQ$Poller. n)]
    (doseq [s socks]
      (.register poller s ZMQ$Poller/POLLIN))
    ;; (trace "polling %s socks" (.getSize poller))
    (.poll poller)
    ;; Randomly take the first ready socket and its message,
    ;; to match core.async's alts! behavior
    (->> (shuffle (range n))
         (filter #(.pollin poller %))
         first
         (.getSocket poller)
         ((juxt receive-all identity)))))

(defn- thread-name [] (.getName (Thread/currentThread)))

(defn- zmq-poller [injector internal-chan zmqt-term]
  (trace "zmq-poller: %s started" (thread-name))
  (loop [socks {:injector injector}, chans {}]
    (let [[val sock] (poll (vals socks))
          id         (get (map-invert socks) sock)
          val        (if (= :injector id) (keyword (String. val)) val)]
      (assert (and id val))
      (trace "zmq-poller: got message from %s" id)
      (match
       [id val]
       
       [:injector :message]
       (let [msg (<!! internal-chan)]
         
         (match
          [msg]
          
          [[:register sock-id new-sock chan-map]]
          (do
            (trace "zmq-poller: registered %s" sock-id)
            (recur (assoc socks sock-id new-sock)
                   (assoc chans sock-id chan-map)))

          [[:close sock-id]]
          (do
            (trace "zmq-poller: closing %s" sock-id)
            (.close (socks sock-id))
            ;; Close chans.
            (doseq [c (chans id)] (close! c))
            (recur (dissoc socks sock-id)
                   (dissoc chans sock-id)))

          [[:command sock-id cmd]]
          (do
            (trace "zmq-poller: commanding %s" sock-id)
            (try
              ;; Execute cmd with socket corresponding to sock-id and
              ;; when return value is non-nil return it to async-control-chan
              ;; for further processing.
              (when-let [res (cmd (socks sock-id))]
                (offer! (get-in chans [sock-id :ctl-out]) res))
              (catch Exception e
                (error "*ERROR* when executing a command: %s" e)))
            (recur socks chans)
            )

          [[sock-id msg-out]]
          (do
            (trace "zmq-poller: sending message to %s" sock-id)
            (try
              (send! (socks sock-id) msg-out)
              (catch Exception e
                (error "*ERROR* when sending a message: %s" e)))
            (recur socks chans)
            )
          )
         )

       [:injector :shutdown]
       (do
         (trace "zmq-poller: shutting down context...")
         (trace "zmq-poller: closing chans...")
         (doseq [c (->> (vals chans)
                        (map #(vals %))
                        flatten)] (close! c))
         (>!! zmqt-term (vals socks)))

       ;; Blows up and leaves resources open. This should never happen.
       [:injector invalid]
       (throw (RuntimeException. (str "Invalid injection message: " invalid)))

       [sock-id msg-in]
       (do
         (trace "zmq-poller: incoming message on %s" sock-id)
         (offer! (get-in chans [sock-id :out]) msg-in)
         (recur socks chans)))))
  
  (trace "zmq-poller: terminated"))

(defn- sock-id-for-chan
  [c pairings]
  (first (for [[id chans] pairings :when ((set (vals chans)) c)] id)))

(defn- injector [injector internal-chan ctl-chan injt-term]
  (let [inject-msg!  (fn [msg]
                       (send! injector "message")
                       (>!! internal-chan msg))
        sock-num (volatile! 0)
        gen-id  (fn [sock]
                  (format "%s-%s-%d"
                          (last (split (thread-name) #"-"))
                          (str (get (map-invert socket-types) (.getType sock)))
                          (vswap! sock-num inc)))]
    (trace "injector: %s started" (thread-name))
    (loop [chans {:control {:in ctl-chan}}]
      (let [in-chans (->> (vals chans)
                          (map #(vals %))
                          flatten)
            [val c] (alts!! in-chans)
            id (sock-id-for-chan c chans)]
        (match
         [id val]

         [:control [:register sock chan-map]]
         (let [sock-id   (gen-id sock)
               in-chans  (select-keys chan-map #{:in :ctl-in})
               out-chans (select-keys chan-map #{:out :ctl-out})]
           (trace "injector: registering new socket %s" sock-id)
           (inject-msg! [:register sock-id sock out-chans])
           (recur (assoc chans sock-id in-chans)))

         ;; Control channel closed => shut down context
         [:control nil]
         (do
           (trace "injector: shutting down context...")
           (doseq [c in-chans] (close! c))
           (send! injector "shutdown"))

         [:control invalid]
         (throw (RuntimeException. (str "Invalid control message: " invalid)))
         
         
         ;; Channel closed => close socket
         [id nil]
         (do
           (trace "injector: closing %s" id)
           (doseq [c (vals (get chans id))] (close! c))
           (inject-msg! [:close id])
           (recur (dissoc chans id)))

         [id msg]
         (do
           (trace "injector: forwarding message to %s" id)
           (cond

             ;; Normal message to deliver via socket.
             (= c (get-in chans [id :in]))
             (inject-msg! [id msg])

             ;; Command message to socket.
             (= c (get-in chans [id :ctl-in]))
             (inject-msg! [:command id msg])

             :else
             (throw (RuntimeException. "This should never happen.")))
           (recur chans))))))
  (close! injt-term)
  (trace "injector: terminated"))

(defn context-alive [ctx]
  (let [{:keys [zmq-thread injector-thread]} ctx]
    (and (.isAlive injector-thread) (.isAlive zmq-thread))))

(defn init-context!
  [ctx]
  (let [{:keys [zmq-thread injector-thread]} ctx]
    (if (context-alive ctx)
      false
      (do
        (.start injector-thread)
        (.start zmq-thread)
        true))))


(defn- bind-rand-port!
  ([sock addr] (bind-rand-port! sock addr 30000))
  ([sock addr timeout-ms]
   (loop []
     (when-let [e (try (.bind sock (str addr ":*")) (catch ZMQException e e))]
       ;; Wait for the OS to release some of the previously bound ports.
       (Thread/sleep timeout-ms)
       (recur)))
   (let [bound-port (-> (.getLastEndpoint sock)
                        String.
                        (split #":")
                        last
                        butlast
                        into-array
                        join
                        Long/parseLong)]
     bound-port)))

(defn context
  "Create a context.
  
  Create a context for sockets that works on top of org.zeromq.ZContext.
  When context is initialized (sockets created on top of it) it will be
  automatically initialized (init-context!).

  org.zeromq.ZContext is a wrapper for org.zeromq.ZMQ$Context. It is basically
  org.zeromq.ZMQ$Context with lifetime logic built in. Sockets don't need to
  be closed manually when using org.zeromq.ZContext."
  ([] (context nil))
  ([name] (context name 1))
  ([name io-threads]
   (let [inj-addr        (str "inproc://injection-" (gensym (or name "")))
         name            (or name (gensym "context"))
         internal-ctx    (ZMQ/context io-threads)
         internal-chan   (chan)
         ctl-chan        (chan)
         zmqt-term       (chan 1)
         injt-term       (chan 1)
         injector-out    (doto (.socket internal-ctx ZMQ/PULL) (.bind inj-addr))
         injector-in     (doto (.socket internal-ctx ZMQ/PUSH) (.connect inj-addr))
         injector-thread (doto (Thread. #(injector injector-in
                                                   internal-chan
                                                   ctl-chan
                                                   injt-term))
                           (.setName (format "injector-%s" name))
                           (.setDaemon true))
         zmq-thread      (doto (Thread.
                                #(zmq-poller injector-out
                                             internal-chan
                                             zmqt-term))
                           (.setName (format "zmq-poller-%s" name))
                           (.setDaemon true))
         ctx             {:internal-ctx    internal-ctx
                          :name            name
                          :ctl-chan        ctl-chan
                          :injector-thread injector-thread
                          :zmq-thread      zmq-thread}
         ;; Returns true on successful shutdown, otherwise nil.
         shutdown        (fn []
                           ;; It is safe to call :shutdown more than once.
                           (when (context-alive ctx)
                             (close! ctl-chan)
                             (<!! injt-term)
                             (let [sockets (<!! zmqt-term)]
                               (doseq [sock sockets] (.close sock)))
                             ;; It is OK to .close a ZMQ$Socket/ZMQ$Context
                             ;; multiple times.
                             (.close injector-out)
                             (.close injector-in)
                             (.close internal-ctx)
                             true))]
     (assoc ctx :shutdown shutdown))))

;; One io-thread should be sufficient almost always.
(def default-context (context "default-context" 1))

(def type-bytes (type (byte-array [])))

(defn- get-bytes [x]
  (if (instance? type-bytes x) x (.getBytes x)))

(defn gen-socket-configurator [opts]
  (fn [socket]
    (let [{:keys [bind connect plain-user plain-pass plain-server
                  zap-domain req-retry id send-hwm recv-hwm
                  subscribe]} opts
          res                 (transient {})]
      (when id (.setIdentity socket (get-bytes id)))
      (when zap-domain (.setZAPDomain (get-bytes zap-domain)))
      (when (or plain-server plain-user plain-pass)
        (if plain-server
          (.setPlainServer socket plain-server)
          (do
            (assert (and plain-user plain-pass)
                    "Specify both :plain-user and :plain-pass")
            (.setPlainUsername socket (get-bytes plain-user))
            (.setPlainPassword socket (get-bytes plain-pass)))))
      (when req-retry
        (.setReqRelaxed socket req-retry)
        (.setReqCorrelate socket req-retry))
      (when send-hwm (.setSndHWM socket send-hwm))
      (when recv-hwm (.setRcvHWM socket recv-hwm))
      (assert (or bind connect) "Specify :bind or :connect.")
      (when bind
        (.bind socket bind)
        (when (re-matches #"tcp://.*:\*" bind)
          (assoc! res :bound-port (-> (.getLastEndpoint socket)
                                      String.
                                      (split #":")
                                      last
                                      butlast
                                      into-array
                                      join))))
      (when connect (.connect socket connect))
      (when subscribe (.subscribe socket (get-bytes subscribe)))
      (persistent! res))))

(defn socket [socket-type & opts]
  (let [opts         (if (map? opts) opts (apply hash-map (apply vector opts)))
        configurator (or (opts :configurator) (gen-socket-configurator opts))
        in           (or (opts :in) (chan))
        out          (or (opts :out) (chan 1))
        ctl-in       (or (opts :ctl-in) (chan))
        ctl-out      (or (opts :ctl-out) (chan 1))
        _            (assert (and in out ctl-in ctl-out) "missing channels")
        ctx          (or (opts :context) default-context)
        initialized  (try (init-context! ctx)
                          (catch java.lang.IllegalThreadStateException e
                            (throw (IllegalArgumentException.
                                    "Context is terminated"))))
        socket       (.socket (ctx :internal-ctx) (socket-types socket-type))
        info         (configurator socket)]
    (>!! (ctx :ctl-chan)
         [:register socket {:in in :out out :ctl-in ctl-in :ctl-out ctl-out}])
    {:in in :out out :ctl-in ctl-in :ctl-out ctl-out :info info}))
