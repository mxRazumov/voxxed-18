(ns voxxed.system
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [troy-west.thimble]
            [troy-west.arche.hugcql :as arche.hugcql]
            [troy-west.arche.integrant :as arche.integrant]
            [qbits.alia.codec.extension.joda-time :refer :all]
            [voxxed.topology :as topology]
            [integrant.core :as ig]))

(defonce state (atom nil))

;;;;;;;;;;;;;
;;; Config

(defmethod ig/init-key :voxxed/stream
  [_ config]
  (topology/initialize config))

(defmethod ig/halt-key! :voxxed/stream
  [_ state]
  (topology/shutdown state))

(defn load-config
  ([]
   (load-config "config.edn"))
  ([file-name]
   (edn/read-string {:readers (merge arche.hugcql/data-readers
                                     arche.integrant/data-readers)}
                    (slurp (io/resource file-name)))))

;;;;;;;;;;;;;;;;
;;; Operation

(defn initialize
  ([]
   (initialize (load-config)))
  ([config]
   (locking state
     (when-not @state
       (reset! state (ig/init config))))))

(defn shutdown
  []
  (locking state
    (when @state
      (ig/halt! @state)
      (reset! state nil))))