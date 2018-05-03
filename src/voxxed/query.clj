(ns voxxed.query
  (:require [clojure.core.async :as async]
            [troy-west.arche :as arche]
            [troy-west.arche.async :as arche.async]
            [voxxed.message :as message]
            [clj-time.periodic :as periodic]
            [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]))

(def default-parallelism 25)

(defn partitions
  [start end]
  (map message/partition
       (periodic/periodic-seq (time.coerce/to-date-time start)
                              (time/plus (time.coerce/to-date-time end) (time/days 1))
                              (time/days 1))))

(defn queries
  [sender word start end]
  (map #(identity {:partition %1
                   :sender    sender
                   :word      word})
       (partitions start end)))

(defn find-word
  [connection sender word start end]
  (let [out (async/chan default-parallelism)]
    (async/pipeline-async
     default-parallelism
     out
     #(arche.async/execute connection
                           :subject/read-word
                           {:values  %1
                            :channel %2})
     (async/to-chan (queries sender word start end)))
    out))

(defn find-words
  [connection sender date]
  (arche/execute
   connection
   :subject/read-words
   {:values {:sender    sender
             :partition (message/partition
                         (time.coerce/to-date-time date))}}))