(ns zmqchans.core
  (:use
   [zmqchans.common])
  (:require
   [zmqchans.utils :refer [attach! terminate!]]
   [clojure.string :refer [split join]]
   [clojure.core.match :refer [match]]
   [clojure.set :refer [map-invert]]
   [clojure.core.async
    :refer [chan  close! go thread <! >! <!! >!! alts!! timeout offer!
            promise-chan]])
  (:import
   [org.zeromq ZMQ ZMQ$Poller ZMQ$Socket ZMQException]
   [zmqchans.common Socket Context]))

(defn- poll
  "Blocking poll that returns a [val, socket] tuple.
  If multiple sockets are ready, one is chosen to be read
  from nondeterministically."
  [socks]
  ;; TODO: what's the perf cost of creating a new poller all the time?
  (let [n      (count socks)
        poller (ZMQ$Poller. n)]
    (doseq [s socks]
      (.register poller s ZMQ$Poller/POLLIN))
    ;; Calls underlying zmq_poll C function. Will signal on events
    ;; that have occurred before this function call and after last
    ;; underlying FD read.
    (.poll poller)
    ;; Randomly take the first ready socket and its message,
    ;; to match core.async's alts! behavior
    (->> (shuffle (range n))
         (filter #(.pollin poller %))
         first
         (.getSocket poller)
         ((juxt receive-all identity)))))

(defn- thread-name [] (.getName (Thread/currentThread)))

(defn- zmq-poller
  "A loop that handles all the interaction with ZMQ Sockets. This library
  is thread safe because all the ZMQ socket manipulation happens on this
  loop that is ran from a single thread."
  [injector internal-chan zmqt-term]
  
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
              (if-let [res (cmd (socks sock-id))]
                (offer! (get-in chans [sock-id :ctl-out]) res)
                (offer! (get-in chans [sock-id :ctl-out]) :nil))
              (catch Exception e
                (offer! (get-in chans [sock-id :ctl-out]) e)))
            (recur socks chans))

          [[sock-id msg-out]]
          (do
            (trace "zmq-poller: sending message to %s" sock-id)
            (try
              (send! (socks sock-id) msg-out)
              (catch Exception e
                (error "*ERROR* when sending a message: %s" e)))
            (recur socks chans))))

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

(defn- injector
  "Injector loop. The thread that runs the injector loop communicates
  with ZMQ poller thread. All input to ZMQ poller thread is injected from this
  thread. Injector has access to one ZMQ socket that sends a signal message
  to ZMQ loop. The real data is transmitted using channels."
  [injector internal-chan ctl-chan injt-term]
  
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
         (let [chan-map (into {} (remove (comp nil? second) chan-map))
               sock-id   (gen-id sock)
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
  (>!! injt-term :terminated)
  (trace "injector: terminated"))

(defn context-alive [ctx]
  (let [{:keys [zmq-thread injector-thread]} ctx]
    (and (.isAlive injector-thread) (.isAlive zmq-thread))))

(def init-context!
  "Initialize a Context. Starts the underlying threads and starts consuming
  more resources than non-initialized context. Context can only be initialized
  once, any subsequent calls shall be no-op."
  ;; Use internal locker to eliminate race conditions.
  (let [locker (Object.)]
    (fn [ctx]
      (locking locker
        (let [{:keys [zmq-thread injector-thread]} ctx]
          (if (context-alive ctx)
            false
            (do
              (.start zmq-thread)
              (.start injector-thread)
              true)))))))

(defn context
  "Create a context.
  
  Create a context that serves as a structure to hold together all the data
  related to running zmqchans. Contains references to the underlying constructs
  like zmq-thread and injector-thread. Manages lifetime of the objects polled
  inside zmq-thread. All the resources will be closed upon termination of
  the context.

  When context is not defined when creating objects (i.e. Socket), default
  context defined on *context* shall be used. If context is not yet initialized
  it shall be initialized on demand.

  Arguments:
  name       - Name of the context, may be used for debugging.
  io-threads - Number of io-threads used by underlying ZMQ context object.
               Default value of 1 should be sufficient almost always.
  "
  ([] (context nil))
  ([name] (context name 1))
  ([name io-threads]
   (let [inj-addr        (str "inproc://injection-" (gensym (or name "")))
         name            (or name (gensym "context"))
         internal-ctx    (ZMQ/context io-threads)
         internal-chan   (chan)
         ctl-chan        (chan)
         zmqt-term       (promise-chan)
         injt-term       (promise-chan)
         injector-out    (doto (.socket internal-ctx ZMQ/PULL)
                           (.bind inj-addr))
         ;;_               (Thread/sleep 10)
         injector-in     (doto (.socket internal-ctx ZMQ/PUSH)
                           ;; Don't send messages until connection is finished.
                           ;; TODO: Investigate if this is necessary.
                           (.setImmediate true)
                           (.connect inj-addr))
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
     ;; Buffer size of 1 is ideal for signaling.
     (when (min-zmq-version 4 0 0)
       (.setConflate injector-out true)
       (.setConflate injector-in true))
     (map->Context (assoc ctx :shutdown shutdown)))))


;; One io-thread should be sufficient almost always.
(def ^:dynamic *context* (context "default-context" 1))

(alter-doc! *context* "Default context to use if no context is defined.")

(defn socket
  "Create a ZMQ socket.

  Arguments:
  socket-type - Socket type to use, see `socket-types`.
  opts        - Optional options

  Optional options:
  First opt can be string defining `attach!` addresses (comma separated).
  After that:
  :context - Context to use instead of default, overrides *context*.
  :eps     - Alternative way to define `attach!` addresses.
  :bind    - Addresses to bind to (string or coll of strings)
  :connect - Addresses to connect to (string or coll of strings)
  :in      - Custom channel for input (default: (chan))
  :out     - Custom channel for output (default: (chan 1000))

  Defining custom input channel is useful if you want to use a transducer.
  Defining custom output channel is useful if you want to use a transducer
  or custom buffering behavior.

  Initializes a context if necessary.
  "
  [socket-type & opts]
  (let [[opts eps] (if (string? (first opts))
                     [(rest opts) (first opts)]
                     [opts nil])
        opts       (if (map? opts) opts (apply hash-map (apply vector opts)))
        ctx        (if (:context opts) (:context opts) *context*)
        eps        (or eps (:eps opts))
        eps        (if eps
                     (if (coll? eps) eps (split eps #","))
                     [])
        bind       (if (:bind opts)
                     (if (coll? (:bind opts))
                       (:bind opts)
                       [(:bind opts)])
                     [])
        connect    (if (:connect opts)
                     (if (coll? (:connect opts))
                       (:connect opts)
                       [(:connect opts)])
                     [])
        eps        (concat eps (map #(str \@ %) bind))
        eps        (concat eps (map #(str \> %) connect))
        in         (or (:in opts) (chan))
        ;; Match size of :out chan to the default
        ;; ZMQ_SNDBUF = 0, ZMQ_SNDHWM = 1000 settings.
        out        (or (:out opts) (if (= socket-type :push)
                                     nil
                                     (chan 1000)))
        ;; ctl-in     (or (:ctl-in opts) (chan))
        ;; ctl-out    (or (:ctl-out opts) (chan 1))
        ;; No good reason to customize ctl-in / ctl-out channels.
        ctl-in     (chan)
        ctl-out    (chan 1)
        _          (assert (and in ctl-in ctl-out) "missing channels")
        
        initialized (try (init-context! ctx)
                         (catch java.lang.IllegalThreadStateException e
                           (throw (IllegalArgumentException.
                                   "Context is terminated"))))
        ;; This might prevent some extremely rare deadlocks from happening.
        ;; TODO: investigate if really needed and why exactly.
        _           (when initialized (Thread/sleep 10))
        socket-r    (.socket (:internal-ctx ctx) (socket-types socket-type))
        socket      (map->Socket {:in    in       :out out :ctl-in
                                  ctl-in :ctl-out ctl-out})]
    ;; We shall never block the poller thread so we don't need a buffer here.
    ;; Buffer of 1 should still be able to carry any number of multipart frames.
    ;; This will save some memory.
    ;; TODO: Is there a perf cost?
    (.setRcvHWM socket-r 1)
    (>!! (:ctl-chan ctx) [:register socket-r socket])
    (try
      (doseq [ep eps] (when-not (attach! socket ep)
                        (throw (IllegalArgumentException.
                                (format "invalid endpoint: %s" ep)))))
      (catch Exception e (terminate! socket) (throw e)))
    socket))

