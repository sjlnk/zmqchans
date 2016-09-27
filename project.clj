(defproject zmqchans "0.3.0"
  :description "A bridge between ZeroMQ sockets and core.async"
  :url "https://github.com/sjlnk/zmqchans"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.zeromq/jzmq "3.1.1-SNAPSHOT"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/core.match "0.3.0-alpha4"]]
  :native-path "lib"
  :test-selectors {:default   (complement :slow)
                   :big-small (fn [m] (= (:name m) 'stress-big-small))}
  :profiles {:dev {:source-paths ["dev"]}}
  )
