(ns zmqchans.test.stress
  (:use
   [zmqchans.core]
   [zmqchans.utils]
   [clojure.test]
   [zmqchans.test.utils])
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

(deftest ^:slow stress-big-small
  (letfn
      [(run-test! [ctx1 ctx2 data-mibs n]
         (let [data-b (byte-array (* 1024 1024 data-mibs))
               len-b  n
               addr-b (str (gensym "inproc://test-big-file-big-"))
               addr-s (str (gensym "inproc://test-big-file-small-"))
               pull-b (doto (socket :pull :context ctx1) (bind! addr-b))
               push-b (doto (socket :push :context ctx1) (connect! pull-b))
               pull-s (doto (socket :pull :context ctx2) (bind! addr-s))
               push-s (doto (socket :push :context ctx2) (connect! pull-s))
               exprom (promise-chan)
               res-b  (thread
                        (loop [i 1]
                          ;;(println "BIG start" i)
                          (send! push-b data-b)
                          (recv! pull-b)
                          ;;(println "BIG end" i)
                          (if (and (< i len-b) (not (poll! exprom)))
                            (recur (inc i))
                            (do (>!! exprom :exit) i))))
               res-s  (thread
                        (loop [i 1]
                          (send! push-s "")
                          (recv! pull-s)
                          ;;(println i)
                          (if (not (poll! exprom))
                            (recur (inc i))
                            i)))]
           (let [res (try [(<!! res-b) (<!! res-s)]
                          (catch java.lang.InterruptedException e
                            (>!! exprom :exit)))]
             (terminate! ctx1)
             (terminate! ctx2)
             res)))]
    (loop [i 0]
      (let [;; single context
          ctx (context)
          [[b1 s1] ms1]    (timed-run-ms (run-test! ctx ctx 100 5))
          perf1 (/ s1 ms1)
          ;; two contexts
          [[b2 s2] ms2]    (timed-run-ms (run-test! (context) (context) 100 5))
          perf2 (/ s2 ms2)]
      (println i perf1 perf2 ms1 ms2)
      (is (> perf2 perf1)))
      (recur (inc i))
      )))
