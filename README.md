# Voxxed Days Melbourne 2018


It's easy, check-out the project then:

```bash
brew install ccm
```

then start a REPL and:

```clojure
;; load data

(def messages (json/decode (slurp (io/resource "messages.json")) true))

;; initialize zookeeper, kafka, cassandra, and kafka-streams

(require '[voxxed.system :as system])

(system/initialize)

;; require kafka ns and load data
(require '[voxxed.kafka :as kafka])

(kafka/send-seq (:thimble/kafka.producer @system/state) "voxxed" messages)

;; require query ns and demonstrate data retrieval

(require '[voxxed.query :as query])

(query/find-words (:arche/connection @system/state) "mark" "1998-12-02T")

(async/<!!
 (async/into []
             (query/find-word
              (:arche/connection @system/state)
              "mark"
              "swap"
              "1998-12-02T"
              "2001-12-22T")))
```

Any problems - shout.
