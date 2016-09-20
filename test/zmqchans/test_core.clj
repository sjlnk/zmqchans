(ns zmqchans.test-core
  (:use
   [zmqchans.core]
   [clojure.test])
  (:require
   [clojure.core.async :refer
    [go go-loop thread close! >!! >! <!! <! chan timeout alts!!]]
   [clojure.set :refer [map-invert]]
   )
  )

(defn random-socket [ctx]
  ;; :router and :rep trigger a bug in libzmq
  ;; https://github.com/zeromq/libzmq/issues/2117
  (socket (rand-nth (remove #{:rep :router} (keys socket-types)))
          :context ctx
          (rand-nth [:bind :connect])
          (str (gensym "inproc://socket"))))

(deftest startup-shutdown
  (let [n 100 ; too high n and inproc runs out of fds
        ctx (context)
        sockets (vec (take n (repeatedly #(random-socket ctx))))]
    (is (.isAlive (ctx :zmq-thread)))
    (is (.isAlive (ctx :injector-thread)))
    (is (= (count (.getSockets (ctx :zcontext))) (+ n 2)))
    ((ctx :shutdown))
    ;; need some time to shut down...
    (Thread/sleep 100)
    (is (not (.isAlive (:zmq-thread ctx))))
    (is (not (.isAlive (:injector-thread ctx))))
    (is (= (count (.getSockets (ctx :zcontext))) 0))
    )
  )

(deftest ping-pong
  (let [ctx (context)
        addr "inproc://test"
        rep (socket :rep :bind addr)
        req (socket :req :connect addr)
        n 1000
        parse (fn [b] (Long/parseLong (String. b)))
        req-l (go-loop [i 0]
                (>!! (req :in) (str i))
                (let [res (parse (<!! (req :out)))]
                  (if (< res n)
                    (recur (inc res))
                    (do
                      (>!! (req :in) (str (inc res)))
                      (close! (req :in))
                      res)
                    )
                  )
                )
        rep-l (go-loop [i (parse (<!! (rep :out)))]
                (>!! (rep :in) (str (inc i)))
                (let [res (parse (<!! (rep :out)))]
                  (if (< res n)
                    (recur (inc res))
                    (do
                      (>!! (rep :in) (str (inc res)))
                      (close! (rep :in))
                      res))
                  )
                )
        req-r (<!! req-l)
        rep-r (<!! rep-l)
        ]
    (is (= (+ req-r rep-r) (+ n n 1)))
    (is (= (count (.getSockets (ctx :zcontext))) 0))
    ((ctx :shutdown))
    ))

;; ping pong xduce

;; stress test (pingpong + socket opener/closer)

;;(ping-pong)
;;(startup-shutdown)
     
                          
         
                   
                    
     
 
 
                                                
  
          
