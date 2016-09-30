(ns zmqchans.utils
  (:use
   [zmqchans.common :exclude [send!]])
  (:require
   [clojure.string :refer [join split]]
   [clojure.core.match :refer [match]]
   [clojure.set :refer [map-invert]]
   [clojure.stacktrace
    :refer [print-stack-trace]]
   [clojure.core.async
    :refer [chan  close! go thread <! >! <!! >!! alts!! timeout offer!
            poll! pipe]])
  (:import
   [org.zeromq ZMQ ZMQ$Poller ZMQ$Socket]
   [zmqchans.common Socket Context]
   ))

(defmulti terminated?
  "Check whether `Socket` or `Context` is terminated. Terminated objects have
  all of their IO resources released and their threads terminated."
  (fn [x] (type x)))

(defmethod terminated? Socket [sock]
  (let [res (>!! (:ctl-in sock) #(comment %))]
    (if res
      (do (<!! (:ctl-out sock)) false)
      true)))
(defmethod terminated? Context [ctx]
  (and (= java.lang.Thread$State/TERMINATED
          (.getState (:zmq-thread ctx)))
       (= java.lang.Thread$State/TERMINATED
          (.getState (:injector-thread ctx)))))

(defn command!
  "Generic way to manipulate a socket. You can use any user defined function
  that will manipulate the underlying `ZMQ$Socket` in a thread safe manner."
  [sock f]
  (>!! (:ctl-in sock) f)
  (let [res (<!! (:ctl-out sock))]
    (cond
      (= res :nil) nil
      (instance? java.lang.Exception res)
      (let [err-msg (with-out-str (print-stack-trace res))]
        (throw (IllegalArgumentException. err-msg)))
      :else res)))

(defn get-last-ep
  "Get `ZMQ_LAST_ENDPOINT`."
  [sock]
  (let [ep (command! sock #(.getLastEndpoint %))]
    (-> ep String. (subs 0 (dec (count ep))))))

(defn get-connectable-local-ep
  "Get a connectable local version of the last endpoint.

  Returns `ZMQ_LAST_ENDPOINT` converted to a connectable format."
  [sock]
  (let [ep (get-last-ep sock)]
    (cond
      (= (count ep) 1) nil
      (re-matches #"tcp://.*" ep)
      (str "tcp://localhost:" (-> ep (split #":") last))
      :else
      ep)))

;; TODO: add support for czmq style random port range specification
(defn bind!
  "Bind a socket to an endpoint."
  [sock ep] (command! sock #(.bind % ep)))

(defn unbind!
  "Unbind a socket from an endpoint.

  When no parameters are given, `ZMQ_LAST_ENDPOINT` is used."
  ([sock] (unbind! sock (get-last-ep sock)))
  ([sock ep] (command! sock #(.unbind % ep))))

(defmulti connect!
  "Connect socket to an endpoint.

  Another socket can be passed as a shortcut."
  (fn [_ target] (type target)))

(defmethod connect! Socket [sock target]
  (let [ep (get-connectable-local-ep target)]
    (command! sock #(.connect % ep))))
(defmethod connect! String [sock target]
  (command! sock #(.connect % target)))

(defn attach!
  "Attach a socket to an endpoint. Can be a bind or connect based on the
  prefix of an endpoint. @ means binding, > means connecting. Inspired by
  CZMQ."
  [sock ep]
  (cond
    (= (first ep) \>) (do (connect! sock (str (subs ep 1))) true)
    (= (first ep) \@) (do (bind! sock (str (subs ep 1))) true)
    :else false))

(defmulti disconnect!
  "Disconnect from an endpoint.

  Another socket can be given as a shortcut. With no parameters,
  `ZMQ_LAST_ENDPOINT` is disconnected."
  (fn [& args] (map #(type %) args)))

(defmethod disconnect! [Socket Socket] [sock target]
  (let [ep (get-connectable-local-ep target)]
    (command! sock #(.disconnect % ep))))
(defmethod disconnect! [Socket String] [sock target]
  (command! sock #(.disconnect % target)))
(defmethod disconnect! [Socket] [sock]
  (let [ep (get-last-ep sock)]
    (command! sock #(.disconnect % ep))))


(defn send!
  "Send message through a socket.

  Doesn't block. If buffers are full,
  will drop the messages with every socket type."
  [sock msg] (>!! (:in sock) msg))

(defn recv!
  "Receive a message from a socket.

  Blocks until a message has been received."
  [sock] (<!! (:out sock)))

(defn try-recv!
  "Receive data but do not block.

  Returns nil if data is not available or socket is closed."
  [sock] (poll! (:out sock)))

(defn recv-all-messages!
  "Receive all messages buffered in an :out channel of a socket."
  [sock]
  (loop [res []]
    (if-let [data (try-recv! sock)] (recur (conj res data)) res)))

(defn setid!
  "Set `ZMQ_IDENTITY` of a socket."
  [sock id]
  (let [id (get-bytes id)]
    (command! sock #(.setIdentity % id))))

(defn getid
  "Get `ZMQ_IDENTITY` of a socket."
  [sock] (command! sock #(.getIdentity %)))

(defn subscribe!
  "Subscribe to a topic.

  Only for SUB sockets."
  [sock topic]
  (command! sock #(.subscribe % (get-bytes topic))))

(defn unsubscribe!
  "Unsubscribe from a topic.

  Only for SUB sockets."
  [sock topic]
  (command! sock #(.unsubscribe % (get-bytes topic))))

(defmulti terminate!
  "Terminate a context or a socket."
  (fn [x] (type x)))

(defmethod terminate! Context [ctx] ((:shutdown ctx)))

(defmethod terminate! Socket [sock] (close! (:in sock)))

(defn proxy!
  "Form a proxy between two sockets. Emulates `ZMQ_PROXY`."
  [x y]
  (pipe (:out x) (:in y) false) (pipe (:out y) (:in x) false))

;; Seems to be broken as of libzmq 4.1.4
;; (defn req-relax! [sock b]
;;   (command! sock (fn [sock]
;;                    (.setReqRelaxed sock b)
;;                    (.setReqCorrelate sock b))))

;; Example transducers, that may be helpful in some situations.

(defn coll-xcept-map [x] (and (coll? x) (not (map? x))))

(defn xform-base [f]
  (map (fn [msg] (if (coll-xcept-map msg) (map f msg) (f msg)))))

(def encode-pprint
  (xform-base #(str (clojure.pprint/write % :stream nil))))

(def decode-str
  (xform-base #(String. %)))

(defmacro with-temp-context
  "Execute body with temporary context bound to *context*.
  Combines `with-open` and `binding` statements."
  [& body]
  `(with-open [ctx# (zmqchans.core/context)]
     (binding [zmqchans.core/*context* ctx#]
       ~@body)))
