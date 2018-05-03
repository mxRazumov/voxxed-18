(ns voxxed.message
  (:refer-clojure :exclude [partition])
  (:require [clojure.tools.logging :as log]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format]
            [troy-west.arche :as arche]
            [clojure.string :as str]
            [cheshire.core :as json]))

(def day-precision (time.format/formatter "yyyyMMdd"))

(defn partition
  [date-time]
  (time.format/unparse day-precision date-time))

(defn subject-tokens
  [subject]
  (->> (str/split subject #" ")
       (map str/lower-case)
       (filter (complement str/blank?))))

(defn digest
  [connection text]
  (try
    (let [{:keys [message-id date from subject]} (json/decode text true)]

      ;; log some output of process (every 1k messages logged)
      (when (= 0 (mod (Integer/parseInt message-id) 1000))
        (log/info "processed 1000 messages" message-id))

      ;; write subject indexes for every token found
      (doseq [token (subject-tokens subject)]
        (arche/execute
         connection
         :subject/write-word
         {:values {:partition  (partition (time.coerce/to-date-time date))
                   :sender     from
                   :word       token
                   :message-id message-id}})))

    (catch Throwable thr
      (log/error thr))))