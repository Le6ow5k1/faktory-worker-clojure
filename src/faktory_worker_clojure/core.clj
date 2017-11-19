(ns faktory-worker-clojure.core
  (:require [faktory-worker-clojure.client :as client]
            [clojure.core.async :as async]
            )
  )

(def heartbeat-period-ms 15000)

(defn start-heartbeat
  [client]
  (let [c (async/chan)]
    (async/go
      (loop []
        (let [[stop? _] (async/alts! [c (async/timeout heartbeat-period-ms)])]
          (if stop?
            stop?
            (try
              (do
                (print (client/beat client))
                (recur))
              (catch Exception e
                (do
                  (print (.getMessage e))
                  (recur)
                  )
                )
              )
            )
          )
        )
      )
    c))

;; (defn do-work
;;   [a]
;;   (print (str "Doing " a))
;;   (print "Done")
;;   )


;; (faktory/perform-async do-work 1)
