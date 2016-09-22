(ns zmqchans.core
  (:require
   [clojure.string :refer [split]]
   [clojure.core.match :refer [match]]
   [clojure.set :refer [map-invert]]
   [clojure.core.async
    :refer [chan  close! go thread <! >! <!! >!! alts!! timeout offer!]])
  (:import
   [org.zeromq ZContext ZMQ ZMQ$Poller ZMQ$Socket]
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
;; (def ^:dynamic *print-trace* true)

(defmacro trace [fmt & args]
  (when *print-trace*
    `(locking print-synchronizer (println (format ~fmt ~@args)))))

(defn- send!
  [^ZMQ$Socket sock msg]
  (let [msg (if (coll? msg) msg [msg])]
    (loop [[head & tail] msg]
      (let [res (.send sock head (if tail
                                   (bit-or ZMQ/NOBLOCK ZMQ/SNDMORE)
                                   ZMQ/NOBLOCK))]
        (cond
          (= false res)
          (let [sock-type (str (get (map-invert socket-types) (.getType sock)))]
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

(defn- zmq-poller [zctx internal-addr internal-chan]
  (let [injector (doto (.createSocket zctx (socket-types :pull))
                   (.bind internal-addr))]
    (trace "zmq-poller: %s started" (thread-name))
    (loop [socks {:injector injector}, chans {}]
      (let [[val sock] (poll (vals socks))
            id         (get (map-invert socks) sock)
            val        (if (= :injector id) (keyword (String. val)) val)
            ]
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
              ;; - Sets socket linger to zctx linger value (default 0).
              ;; - Calls .close on socket.
              ;; - Removes socket from zctx.
              ;; - Should never block?
              (.destroySocket zctx (socks sock-id))
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
           ;; - Sets linger on all sockets to 0 (by default) and closes them.
           ;; - Removes sockets from the context.
           ;; - Terminates the underlying context.
           ;; - If linger of zctx is > 0, may block if any sockets
           ;;   (even closed) have pending operations.
           ;; - Linger values on individual sockets are not respected.
           ;;(doseq [s socks] (.destroySocket zctx s))
           (.destroy zctx)
           ;; Close chans.
           (trace "zmq-poller: closing chans...")
           (doseq [c (->> (vals chans)
                          (map #(vals %))
                          flatten)] (close! c)))

         [:injector invalid]
         (throw (RuntimeException. (str "Invalid injection message: " invalid)))

         [sock-id msg-in]
         (do
           (trace "zmq-poller: incoming message on %s" sock-id)
           (offer! (get-in chans [sock-id :out]) msg-in)
           (recur socks chans)
           )
         ))
      )
    )
  (trace "zmq-poller: terminated")
  )

(defn- sock-id-for-chan
  [c pairings]
  (first (for [[id chans] pairings :when ((set (vals chans)) c)] id)))

(defn- injector [zctx internal-addr internal-chan ctl-chan]
  (let [injector (doto (.createSocket zctx (socket-types :push))
                   (.connect internal-addr))
        inject-msg!  (fn [msg]
                       (send! injector "message")
                       (>!! internal-chan msg))
        sock-num (volatile! 0)
        gen-id  (fn [sock]
                  (format "%s-%s-%d"
                          (last (split (thread-name) #"-"))
                          (str (get (map-invert socket-types) (.getType sock)))
                          (vswap! sock-num inc)))
        ]
    (trace "injector: %s started" (thread-name))
    (loop [chans {:control {:in ctl-chan}}]
      (let [in-chans (->> (vals chans)
                          (map #(vals %))
                          flatten)
            [val c] (alts!! in-chans)
            id (sock-id-for-chan c chans)
            ]
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
             (throw (RuntimeException. "This should never happen."))
             )
           (recur chans))))))
  
  (trace "injector: terminated"))

(defn init-context!
  [ctx]
  (let [{:keys [zmq-thread injector-thread]} ctx]
    (if (.isAlive injector-thread)
      false
      (do
        (.start injector-thread)
        (.start zmq-thread)
        true))))

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
   (let [injection-addr  (str "ipc://injection-" (gensym (or name "")))
         name            (or name (gensym "context"))
         zcontext        (doto (ZContext.)
                           (.setIoThreads io-threads)
                           (.setMain true) ; causes ZMQ$Context to term
                           )
         internal-chan   (chan)
                         
         ctl-chan        (chan) 
         injector-thread (doto (Thread.
                                #(injector zcontext injection-addr
                                           internal-chan ctl-chan))
                           (.setName (format "injector-%s" name))
                           (.setDaemon true))
         zmq-thread      (doto (Thread.
                                #(zmq-poller zcontext injection-addr
                                             internal-chan))
                           (.setName (format "zmq-poller-%s" name))
                           (.setDaemon true))
         shutdown        (fn [] (close! ctl-chan))
         ]
     {:zcontext        zcontext
      :name            name
      :ctl-chan        ctl-chan
      :addr            injection-addr
      :injector-thread injector-thread
      :zmq-thread      zmq-thread
      :shutdown        shutdown}
     )))

;; One io-thread should be sufficient almost always.
(def default-context (context "default-context" 1))

(def type-bytes (type (byte-array [])))

(defn get-bytes [x]
  (if (instance? type-bytes x) x (.getBytes x)))

(defn gen-socket-configurator [opts]
  (fn [socket]
    (let [{:keys [bind connect plain-user plain-pass plain-server
                  zap-domain req-retry id send-hwm recv-hwm
                  subscribe]} opts]
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
      ;; (when plain-user
      ;;   (assert (and (coll? plain) (= (count plain) 2))
      ;;           "Specify :plain-user in form [username password]")
      ;;   (.setPlainUsername s (.getBytes (first plain)))
      ;;   (.setPlainPassword s (.getBytes (second plain)))
      ;;   )
      (when send-hwm (.setSndHWM socket send-hwm))
      (when recv-hwm (.setRcvHWM socket recv-hwm))
      (assert (or bind connect) "Specify :bind or :connect.")
      (when bind (.bind socket bind))
      (when connect (.connect socket connect))
      (when subscribe (.subscribe socket (get-bytes subscribe))))))

(defn socket [socket-type & opts]
  (let [opts         (if (map? opts) opts (apply hash-map (apply vector opts)))
        configurator (or (opts :configurator) (gen-socket-configurator opts))
        in           (or (opts :in) (chan))
        out          (or (opts :out) (chan 1))
        ctl-in       (or (opts :ctl-in) (chan))
        ctl-out      (or (opts :ctl-out) (chan 1))
        ctx          (or (opts :context) default-context)
        initialized  (try (init-context! ctx)
                                (catch java.lang.IllegalThreadStateException e
                                  (throw (IllegalArgumentException.
                                          "Context is terminated"))))]
    (let [socket (.createSocket (ctx :zcontext) (socket-types socket-type))]
      (assert (and in out ctl-in ctl-out) "missing channels")
      (configurator socket)
      (>!! (ctx :ctl-chan)
           [:register socket {:in in :out out :ctl-in ctl-in :ctl-out ctl-out}])
      {:in in :out out :ctl-in ctl-in :ctl-out ctl-out}
      )))

(comment
  
  )



