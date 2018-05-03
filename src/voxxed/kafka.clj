(ns voxxed.kafka
  (:require [troy-west.thimble.kafka :as thimble.kafka]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]))

(defn send-seq
  [producer topic messages]
  (log/infof "sending %s messages" (count messages))
  (doseq [message messages]
    (thimble.kafka/send-message producer
                                topic
                                (:message-id message)
                                (json/encode message)))
  (log/info "messages sent"))