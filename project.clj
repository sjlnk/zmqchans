(defproject zmqchans/zmqchans "0.3.3"
  :description "Thread-safe Clojure core.async interface to ZeroMQ"
  :url "https://github.com/sjlnk/zmqchans"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.zmapi/jzmq "f831846-SNAPSHOT"]
                 [org.zmapi/jzmq-linux-x86_64 "f831846-4.1.5"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/core.match "0.3.0-alpha4"]]
  :native-path "lib"
  :test-selectors {:default   (complement :slow)
                   :big-small (fn [m] (= (:name m) 'stress-big-small))}
  :profiles {:dev {:source-paths ["dev"]}}
  )
