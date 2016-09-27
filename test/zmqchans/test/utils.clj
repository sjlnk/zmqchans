(ns zmqchans.test.utils
  (:use
   [zmqchans.core]
   [zmqchans.utils]
   [clojure.test])
  (:require
   [clojure.string :refer [split join]]
   [clojure.core.async :refer
    [go go-loop thread close! >!! >! <!! <! chan timeout alts!!
     offer! poll! promise-chan]]
   [clojure.set :refer [map-invert]]
   )
  (:import
   [org.zeromq ZMQ ZMQ$Poller ZMQ$Socket ZMQException]
   )
  )

(defn get-pid []
  (-> (java.lang.management.ManagementFactory/getRuntimeMXBean) .getName))

(defn run-with-timeout
  "Run a function on another thread with maximum execution duration.

  f             - function to execute, takes return channel as an argument
  timeout-ms    - milliseconds to wait before timeout
  stop-delay-ms - milliseconds to wait between .interrupt and .stop calls
                  (default: 1000)

  Optional result of the function will be put to `:res` chan and
  execution status is always returned to `:status` chan.

  Will try nicely just `.interrupt` the thread first. This can be caught
  from the other thread to facilitate a controlled way to exit.
  If `.interrupt` doesn't kill the thread (after `stop-delay-ms`),
  `.stop` will be called. .interrupt can be caught from the thread to allow a
  controlled way to exit. Sometimes threads just refuse to die. Not sure
  if there are any good solutions for this.

  Make sure to design the function in a way that stopping won't cause
  undefined behavior.

  If function `f` spawns new threads, those are not managed by this function
  and may continue running indefinitely if their life cycle is not managed
  properly in `f`."
  ([f timeout-ms] (run-with-timeout f timeout-ms 1000))
  ([f timeout-ms stop-delay-ms]
   (let [res-ch    (chan 1)
         status-ch (chan 1)
         name      (str (gensym "run-with-timeout-"))
         inf-ex    (atom nil)
         launcher  (bound-fn []
                     (let [t (Thread.
                              (bound-fn []
                                (when-let [res (try (f)
                                               (catch Exception e
                                                 (reset! inf-ex e)))]
                                  (>!! res-ch res)))
                              (str name "-inferior"))]
                       (.start t)
                       (.join t timeout-ms)
                       (if (.isAlive t)
                         (do
                           ;; Interrupt if thread is waiting/sleeping,
                           ;; will throw an exception in the target thread.
                           ;; Should kill deadlocked threads but cannot kill
                           ;; a thread that is simply executing code.
                           (.interrupt t)
                           (Thread/sleep stop-delay-ms)
                           (if (.isAlive t)
                             (do
                               ;; Stops a thread forcefully.
                               ;; Risky if this thread is
                               ;; sharing data with other threads.
                               (.stop t)
                               (>!! status-ch :stop))
                             (>!! status-ch :interrupt)))
                         (if @inf-ex
                           (>!! status-ch @inf-ex)
                           (>!! status-ch :normal)))))
         t         (Thread. launcher name)]
     (.start t)
     {:res res-ch :status status-ch})))

(defmacro deftest-with-timeout
  "Defines a test function with maximum running time.

  Uses `run-with-timeout` to run contents of `body` with timeout `timeout-ms`.
  Creates an `is` statement at the end to check that function exited normally,
  which means no timeout or exceptions.

  Will try to `.interrupt` or eventually `.stop` the thread if timeout has
  reached but sometimes threads just refuse to die. Not sure if there are any
  good solutions for this.

  Unhandled exceptions will be delegated back to the calling thread for the
  test runner to process them normally.

  If `body` creates new threads, those are not managed by this macro. They
  will keep on running indefinitely if their life cycle is not properly
  managed."
  [name timeout-ms & body]
  (when *load-tests*
    (let [f (gensym "f")]
      `(let [~f (fn []
                  (let [res# (run-with-timeout (fn [] ~@body) ~timeout-ms)
                        res# (<!! (res# :status))]
                    (if (instance? java.lang.Exception res#)
                      (throw res#)
                      (is (= :normal res#)))
                    )
                  )]
         (def ~(vary-meta name assoc :test f)
           (fn [] (test-var (var ~name))))))))

(defmacro timed-run-ms [& forms]
  `(let [start-time# (System/nanoTime)
         res# (do ~@forms)
         end-time# (System/nanoTime)]
     [res# (double (/ (- end-time# start-time#) 1e6))]))
