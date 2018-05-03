(ns voxxed.topology
  (:require [voxxed.message :as message])
  (:import (org.apache.kafka.streams KafkaStreams StreamsConfig StreamsBuilder)
           (org.apache.kafka.streams.kstream ForeachAction)))

(defn initialize
  [{:keys [topic stream-config connection]}]
  (let [builder      (StreamsBuilder.)]
    (.foreach (.stream builder ^String topic)
              (reify ForeachAction
                (apply [_ _ text]
                  (message/digest connection text))))
    (let [k-stream (KafkaStreams. (.build builder) (StreamsConfig. stream-config))]
      (.start k-stream)
      k-stream)))

(defn shutdown
  [k-stream]
  (.close ^KafkaStreams k-stream))