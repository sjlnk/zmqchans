(ns zmqchans.test.core
  (:use
   [zmqchans.core]
   [zmqchans.utils]
   [zmqchans.test.utils]
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

;; Many of these tests are trying induce deadlocks. Some tests
;; don't have many explicit "is" statements because there is not much
;; to check. These tests have a timeout limit: they must not freeze
;; or they will fail an implicit "is" statement.

(defn random-socket [ctx]
  ;; :router and :rep trigger a bug in libzmq
  ;; when connecting without binding first.
  ;; See: https://github.com/zeromq/libzmq/issues/2117
  (socket (rand-nth (remove #{:rep :router}
                            (keys zmqchans.common/socket-types)))
          :context ctx
          (rand-nth [:bind :connect])
          (str (gensym "inproc://test-random-socket-"))))

(deftest-with-timeout startup-shutdown 1000
  (let [n       100 ; too high n and inproc runs out of fds
        ctx     (context)
        sockets (vec (take n (repeatedly #(random-socket ctx))))]
    (is (context-alive? ctx))
    (terminate! ctx)
    (Thread/sleep 100)
    (is (terminated? ctx))))

(deftest-with-timeout deadlock-seeker 4000
  (let [ctx  (context)
        addr (str (gensym "inproc://test-deadlock-seeker-"))
        pub  (doto (socket :pub :context ctx) (bind! addr))
        n-sub 10
        subs (for [i (range n-sub)]
               (doto (socket :sub :context ctx)
                 (connect! pub)
                 (subscribe! (str (mod i 10)))))
        rep (socket :rep (str (gensym "@inproc://test-rep-")) :context ctx)
        req (doto (socket :req :context ctx) (connect! rep))
        ex-prom (promise-chan)
        tstamps (atom {})
        pp-term (thread
                  (loop []
                    (when-not (poll! ex-prom)
                      (send! req "ping?")
                      (when-let [msg (recv! rep)]
                        (assert (= (String. msg) "ping?")))
                      (send! rep "pong!")
                      (when-let [msg (recv! req)]
                        (assert (= (String. msg) "pong!")))
                      (swap! tstamps #(assoc % :pp (java.util.Date.)))
                      (recur))))
        pub-term (thread
                   (loop [i 0]
                     (when-not (poll! ex-prom)
                       (do
                         (send! pub (str (mod i 10)))
                         (recur (inc i))))))
        sub-term (thread
                   (let [chans (concat [ex-prom] (map #(:out %) subs))]
                     (loop [[v c] (alts!! chans)]
                       (when (not= c ex-prom)
                         (let [i (Long/parseLong (String. v))]
                           (swap! tstamps #(assoc % i (java.util.Date.))))
                         (recur (alts!! chans))))))
        ext-term (thread
                   (loop [i 0]
                     (when (not (poll! ex-prom))
                       (let [sock (doto (socket :sub :context ctx)
                                    (connect! pub)
                                    (subscribe! (str (mod i 10))))]
                         (terminate! sock))
                       (recur (inc i)))))]
    (Thread/sleep (* 2000 1))
    (>!! ex-prom :exit)
    (<!! pub-term)
    (<!! sub-term)
    (<!! ext-term)
    (terminate! ctx)
    (is (= (inc (min 10 n-sub)) (count @tstamps)))
    (let [now (java.util.Date.)
          deltas (map #(- (.getTime now) (.getTime %)) (vals @tstamps))]
      ;;(println "max delta: " (apply max deltas))
      (is (< (apply max deltas) 400)
          "possible deadlock, too long time passed since last update"))))

(deftest-with-timeout ping-pong 2000
  (let [ctx   (context)
        addr  (str (gensym "inproc://test-ping-pong-"))
        rep   (doto (socket :rep :context ctx) (bind! addr))
        req   (doto (socket :req :context ctx) (connect! rep))
        n     1000
        parse (fn [b] (Long/parseLong (String. b)))
        rep-l (go-loop [i (parse (recv! rep))]
                ;;(println "rep" i)
                (when (<= i n)
                  (send! rep (str (inc i))))
                (if (< i n)
                  (recur (parse (recv! rep)))
                  (do (terminate! rep) i)))
        req-l (go-loop [i -1]
                ;;(println "req" i)
                (when (<= i n)
                  (send! req (str (inc i))))
                (if (< i n)
                  (recur (parse (recv! req)))
                  (do (terminate! req) i)))
        req-r (<!! req-l)
        rep-r (<!! rep-l)]
    (is (= (min rep-r req-r) n))
    (is (= (Math/abs (- rep-r req-r)) 1))
    (terminate! ctx)))


;; If one context is processing huge messages, it's more efficient to transfer
;; smaller messages in another context.
(deftest-with-timeout big-small 10000
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
    (let [;; single context
          ctx           (context)
          [[b1 s1] ms1] (timed-run-ms (run-test! ctx ctx 50 5))
          perf1         (/ s1 ms1)
          ;; two contexts
          [[b2 s2] ms2] (timed-run-ms (run-test! (context) (context) 50 5))
          perf2         (/ s2 ms2)]
      (is (> perf2 perf1)))))

(deftest-with-timeout xsub-xpub 2000
  (binding [*context* (context)]
    (let [xsub   (socket :xsub "@inproc://xsub")
          xpub   (doto (socket :xpub "@inproc://xpub") (proxy! xsub))
          n-pubs 20
          n-subs 100
          pubs   (vec (for [i (range n-pubs)]
                        [i (doto (socket :pub) (connect! xsub))]))
          subs   (vec (for [i (range n-subs)]
                        [i (doto (socket :sub)
                             (connect! xpub)
                             (subscribe! (str (mod i n-pubs))))]))
          s-term (vec (for [[i s] subs]
                        (thread
                          ;;(locking *out* (println i "listening"))
                          (let [res (Long/parseLong (String. (recv! s)))]
                            (assert (= (mod i n-pubs) res))))))]
      ;; Not sure if necessary.
      (Thread/sleep 10)
      (doseq [[i p] pubs] (send! p (str i)))
      (doseq [tc s-term] (<!! tc)))
    (terminate! *context*)
    (Thread/sleep 100)
    (is (terminated? *context*))))

(deftest-with-timeout proxy-chain 5000
  (binding [*context* (context)]
    (let [n-proxies 5
          n-msg 100
          rep (socket :rep "@tcp://*:*")
          ppairs (for [_ (range n-proxies)] [(socket :dealer)
                                    (socket :router "@tcp://*:*")])
          req (socket :req)]
      (doseq [[d r] ppairs] (proxy! d r))
      (connect! (first (first ppairs)) rep)
      (doseq [[[_ r1] [d2 r2]] (partition 2 1 ppairs)]
        (connect! d2 r1))
      (connect! req (second (last ppairs)))
      (dotimes [i n-msg]
        (send! req (str i))
        (assert (= (String. (recv! rep)) (str i)))
        (send! rep (str i))
        (assert (= (String. (recv! req)) (str i)))))))

(deftest-with-timeout buffering-test-1 5000
  (binding [*context* (context)]
    (let [addr "inproc://buffering-test"
          n-msg 500
          msg-len 20
          s1 (socket :dealer :connect addr)
          s2 (socket :dealer :out (chan 1000))]
      (doseq [i (range n-msg)]
        (send! s1 (repeat msg-len (str i))))
      (bind! s2 addr)
      (Thread/sleep 100)
      (is (= n-msg (count (recv-all-messages! s2))))))
  (terminate! *context*))


(deftest-with-timeout buffering-test-2 5000
  (binding [*context* (context)]
    (let [addr "inproc://buffering-test"
          n-msg 500
          msg-len 20
          s1 (socket :dealer :bind addr)
          s2 (socket :dealer :connect addr :out (chan 1000))]
      (doseq [i (range n-msg)]
        (send! s1 (repeat msg-len (str i))))
      (Thread/sleep 100)
      (is (= n-msg (count (recv-all-messages! s2))))))
  (terminate! *context*))

(deftest-with-timeout pprint-xform-test 1000
  (binding [*context* (context)]
    (let [addr "inproc://pprint-xform-test"
          s #(socket :pair
                     :in (chan 1 encode-pprint)
                     :out (chan 1000 decode-str))
          s1 (doto (s) (bind! addr))
          s2 (doto (s) (connect! addr))]
      (send! s1 123)
      (is (= "123" (recv! s2)))
      (send! s1 [123 [111]])
      (is (= ["123" "[111]"] (recv! s2)))
      (send! s1 {:a 111})
      (is (= "{:a 111}" (recv! s2))))
    (terminate! *context*)))

(deftest-with-timeout stream-test 1000
  (binding [*context* (context)]
    (let [n-clients 10
          host        (socket :stream :bind "tcp://*:*")
          clients        (for [_ (range n-clients)] (socket :stream))]
      (let [con (fn [c b]
                  (connect! c b)
                  [(first (recv! c)) (first (recv! b))])
            ids (vec (for [c clients] (con c host)))]
        (doseq [[i [id1 id2]] (map-indexed vector ids)]
          (let [c (nth clients i)]
            (send! c [id1 (str (vec id1))])
            (assert (str (vec id1)) (String. (second (recv! host))))
            (send! host [id2 (str (vec id2))])
            (assert (str (vec id2)) (String. (second (recv! c))))
            ))
        (doseq [c clients] (disconnect! c))))))

(deftest-with-timeout reconnect-rebind 1000
  (binding [*context* (context)]
    (let [rep (socket :rep)
          req (socket :req)]
      (dotimes [_ 100]
        (bind! rep "tcp://*:*")
        (connect! req rep)
        (send! req "ping?")
        (assert (= (String. (recv! rep)) "ping?"))
        (send! rep "pong!")
        (assert (= (String. (recv! req)) "pong!"))
        (unbind! rep)
        (disconnect! req)))))














;; Relaxed REQ option is broken as of libzmq 4.1.4
;; (deftest-with-timeout relaxed-req 2000
;;   (binding [*context* (context)]
;;     (let [addr "inproc://relaxed-req-rep"
;;           router (socket :router)
;;           req (doto (socket :req :connect addr) (req-relax! true))
;;           ]
;;       (send! req "1")
;;       (send! req "2")
;;       ;;(dotimes [i 5] (send! req (str i)) (Thread/sleep 100))
;;       ;;(dotimes [i 3] (recv! router))
;;       )
;;     (terminate! *context*)
;;     ))
