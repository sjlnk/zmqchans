(defproject zmqchans "0.1.0-SNAPSHOT"
  :description "Control ZeroMQ sockets using core.async"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.zeromq/jzmq "3.1.0"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 ]
  :native-path "lib"
  :profiles {:dev {:source-paths ["dev"]}}
  )
