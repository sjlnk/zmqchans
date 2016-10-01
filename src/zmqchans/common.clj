(ns zmqchans.common
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

(defmacro alter-doc! [name docstring]
  `(alter-meta! (var ~name) merge {:doc ~docstring}))

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
   :stream 11
   })

(defrecord Socket [in out ctl-in ctl-out]
  java.io.Closeable
  (close [this] (close! (:in this))))

(defrecord Context [internal-ctx name ctl-chan
                    inj-thread zmq-thread shutdown]
  java.io.Closeable
  (close [this] ((:shutdown this))))

(defn min-zmq-version [major minor patch]
  (>=  (ZMQ/getFullVersion) (ZMQ/makeVersion major minor patch)))

;; Prevent overlapping lines printed from multiple threads.
(def print-synchronizer (Object.))

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

(def type-bytes (type (byte-array [])))

(defn get-bytes [x]
  (if (instance? type-bytes x) x (.getBytes x)))

(defn send!
  [^ZMQ$Socket sock msg]
  (let [msg (if (coll? msg) msg [msg])]
    (loop [[head & tail] msg]
      ;; Investigate whether error catching here is a good idea or whether
      ;; the thread should be allowed to crash in an erroneous condition.
      (let [res (try (.send sock head (if tail
                                        (bit-or ZMQ/NOBLOCK ZMQ/SNDMORE)
                                        ZMQ/NOBLOCK))
                     (catch ZMQException e e))]
        (cond
          (instance? Exception res)
          (error "*ERROR* message not sent on %s: %s" sock res)
          (= false res)
          (let [sock-type (str (get (map-invert socket-types)
                                    (try (.getType sock)
                                         (catch ZMQException e nil))))]
            (error "*ERROR* message not sent on %s (%s)" sock sock-type))
          tail (recur tail))))))

(defn receive-all
  "Receive all data parts from the socket, returning a vector of byte arrays.
  If the socket does not contain a multipart message,
  returns a plain byte array."
  [^ZMQ$Socket sock]
  (loop [acc (transient [])]
    (let [new-acc (conj! acc (.recv sock))]
      (if (.hasReceiveMore sock)
        (recur new-acc)
        (let [res (persistent! new-acc)]
          (if (= 1 (count res))
            (first res) res))))))
