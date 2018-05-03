(ns voxxed.solution
  (:refer-clojure :exclude [partition])
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [troy-west.arche :as arche]
            [clj-time.coerce :as time.coerce]
            [clojure.string :as str]
            [clj-time.format :as time.format]))

(def day-precision (time.format/formatter "yyyyMMdd"))

(defn partition
  [text]
  (->> (time.coerce/to-date-time text)
       (time.format/unparse day-precision)))

(defn digest
  [connection text]
  (try
    (let [{:keys [message-id date from subject]} (json/decode text true)]

      ;; log some output of process (every 1k messages logged)
      (when (and (not= "0" message-id)
                 (= 0 (mod (Integer/parseInt message-id) 1000)))
        (log/info "processed 1000 messages" message-id))

      ;; write subject indexes for every token found
      (let [tokens (->> (str/split subject #" ")
                        (map str/lower-case)
                        (filter (complement str/blank?)))]
        (doseq [token tokens]
          (arche/execute connection
                         :subject/write-word
                         {:values {:partition  (partition date)
                                   :sender     from
                                   :word       token
                                   :message-id message-id}}))))

    (catch Throwable thr
      (log/error thr))))