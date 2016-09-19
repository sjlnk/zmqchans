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
   :push   ZMQ/PUSH})

(def ^:dynamic *print-stderr* true)

(defn error [fmt & args]
  (binding [*out* *err*] (print (apply format (str fmt "\n") args))))

(defn send!
  [^ZMQ$Socket sock msg]
  (let [msg (if (coll? msg) msg [msg])]
    (loop [[head & tail] msg]
      ;;TODO: handle byte buffers.
      (let [res (.send sock head (if tail
                                   (bit-or ZMQ/NOBLOCK ZMQ/SNDMORE)
                                   ZMQ/NOBLOCK))]
        (cond
          (= false res) (error "*ERROR* message not sent on %s" sock)
          tail (recur tail))))))

(defn receive-all
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

(defn poll
  "Blocking poll that returns a [val, socket] tuple.
  If multiple sockets are ready, one is chosen to be read from nondeterministically."
  [socks]
  ;; TODO: what's the perf cost of creating a new poller all the time?
  (let [n      (count socks)
        poller (ZMQ$Poller. n)]
    (doseq [s socks]
      (.register poller s ZMQ$Poller/POLLIN))
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
    (loop [socks {:injector injector}, chans {}]
      (let [[val sock] (poll (vals socks))
            id         (get (map-invert socks) sock)
            val        (if (= :injector id) (keyword (String. val)) val)
            ]
        (assert (and id val))
        (match
         [id val]
         
         [:injector :message]
         (let [msg (<!! internal-chan)]
           
           (match
            [msg]
            
            [[:register sock-id new-sock chan-map]]
            (do
              (println "zmq-poller: registered " sock-id)
              (recur (assoc socks sock-id new-sock)
                     (assoc chans sock-id chan-map)))

            [[:close sock-id]]
            (do
              (println "zmq-poller: closing " sock-id)
              (.close (socks sock-id))
              (doseq [c (chans id)] (close! c))
              (recur (dissoc socks sock-id)
                     (dissoc chans sock-id)))

            [[:command sock-id cmd]]
            (do
              (println "zmq-poller: commanding " sock-id)
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
              (println "zmq-poller: sending message to " sock-id)
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
           (println "zmq-poller: shutting down context...")
           (doseq [sock (vals socks)] (.close sock))
           (doseq [c (->> (vals chans)
                          (map #(vals %))
                          flatten)] (close! c)))

         [:injector invalid]
         (throw (RuntimeException. (str "Invalid injection message: " invalid)))

         [sock-id msg-in]
         (do
           (println "zmq-poller: incoming message on " sock-id)
           (offer! (get-in chans [sock-id :out]) msg-in)
           (recur socks chans)
           )
         ))
      )
    )
  (println "zmq-poller: terminated")
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
           (println "injector: registering new socket " sock-id)
           (inject-msg! [:register sock-id sock out-chans])
           (recur (assoc chans sock-id in-chans))
           )

         ;; Control channel closed => shut down context
         [:control nil]
         (do
           (println "injector: shutting down context...")
           (doseq [c in-chans] (close! c))
           (send! injector "shutdown")
           )

         [:control invalid]
         (throw (RuntimeException. (str "Invalid control message: " invalid)))
         
         
         ;; Channel closed => close socket
         [id nil]
         (do
           (println "injector: closing " id)
           (doseq [c (vals (get chans id))] (close! c))
           (inject-msg! [:close id])
           (recur (dissoc chans id))
           )

         [id msg]
         (do
           (println "injector: forwarding message to " id)
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
           (recur chans)
           ))
        )
      )
    )
  (println "injector: terminated")
  )

(defn- init-context!
  [ctx]
  (let [{:keys [zmq-thread injector-thread]} ctx]
    (when-not (.isAlive zmq-thread) (.start zmq-thread))
    (when-not (.isAlive injector-thread) (.start injector-thread)))
  )

(defn context
  ([] (context nil))
  ([name] (context name 1))
  ([name io-threads]
   (let [injection-addr (str "inproc://injection-" (gensym (or name "")))
         name (or name (gensym "context"))
         zcontext (doto (ZContext.) (.setIoThreads io-threads))
         internal-chan (chan)
         ctl-chan (chan)
         injector-thread (doto (Thread.
                                #(injector zcontext injection-addr
                                           internal-chan ctl-chan))
                           (.setName (format "injector-%s" name))
                           (.setDaemon true)
                           )
         zmq-thread (doto (Thread.
                           #(zmq-poller zcontext injection-addr
                                        internal-chan))
                           (.setName (format "zmq-poller-%s" name))
                           (.setDaemon true)
                           )
         ]
     {:zcontext        zcontext
      :name            name
      :ctl-chan        ctl-chan
      :addr            injection-addr
      :injector-thread injector-thread
      :zmq-thread      zmq-thread}
     )
   )
  )

;; One io-thread should be sufficient almost always.
(def default-context (context "default-context" 1))

(defn gen-socket-configurator [opts]
  (fn [socket]
    (let [{:keys [bind connect configurator]} opts]
      (assert (or bind connect) "Specify :bind or :connect.")
      (when bind (.bind socket bind))
      (when connect (.connect socket connect))
      ))
  )

(defn socket [socket-type & opts]
  (let [opts         (if (map? opts) opts (apply hash-map (apply vector opts)))
        configurator (or (opts :configurator) (gen-socket-configurator opts))
        in           (or (opts :in) (chan))
        out          (or (opts :out) (chan 1))
        ctl-in       (or (opts :ctl-in) (chan))
        ctl-out      (or (opts :ctl-out) (chan 1))
        ctx          (try (doto (or (opts :context) default-context) init-context!)
                          (catch java.lang.IllegalThreadStateException e
                            (throw (IllegalArgumentException.
                                    "Context is terminated"))))
        socket       (.createSocket (ctx :zcontext) (socket-types socket-type))
        ]
    (assert (and in out ctl-in ctl-out) "missing channels")
    (configurator socket)
    (>!! (ctx :ctl-chan)
         [:register socket {:in in :out out :ctl-in ctl-in :ctl-out ctl-out}])
    {:in in :out out :ctl-in ctl-in :ctl-out ctl-out}
    )
  )

(comment
  (let [sock (.createSocket (default-context :zcontext) ZMQ/DEALER)]
    (.getType sock)
    )
  )



