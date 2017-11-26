(ns faktory-worker-clojure.core
  (:require [faktory-worker-clojure.client :as client]
            [clojure.core.async :as async]
            )
  (:import java.util.concurrent.Executors)
  )

(def heartbeat-period-ms 15000)
(def default-pool-size 25)
(def default-queues [:default])
(def job-fns (atom {}))

(defn try-beat
  [client]
  (try
    (client/beat client)
    (catch Exception e
      (print (.getMessage e)))))

(defn start-heartbeat
  [client]
  (let [c (async/chan)]
    (async/go
      (loop []
        (let [[stop? _] (async/alts! [c (async/timeout heartbeat-period-ms)])]
          (if stop?
            stop?
            (do
              (try-beat client)
              (recur))))))
    c))

(defn create-worker-pool
  [size]
  (Executors/newFixedThreadPool size))

(defprotocol WorkerManager
  (start [this])
  (stop [this])
  )

(defn- process-job
  [client {:keys [jobtype jid args]}]
  (try
      (let [job-fn (get @job-fns jobtype)]
        (apply job-fn args)
        (client/ack client jid)
        )
      (catch Exception e
        (do
          (client/fail client jid e)
          (throw e)
          )
        )
      )
  )

(defn fetch-and-process
  [client]
  (when-let [job (client/fetch default-queues)]
    (process-job client job)
    )
  )

(defn run-worker
  [ch client]
  (async/thread
    (loop []
      (let [[event _] (async/alts! [ch (async/timeout 100)])]
        (case event
          :terminate nil
          (do (fetch-and-process client)
              (recur))
          )
        )
      )
    )
  )

(defn create-worker-manager
  ([client] (create-worker-manager client default-pool-size))
  ([client pool-size]
   (let [pool (create-worker-pool pool-size)
         worker-chans (repeat pool-size (async/chan))]
     (reify WorkerManager
       (start [this]
         (doseq [ch worker-chans]
           (run-worker ch client)
           )
         )
       (stop [this]
         (doseq [ch worker-chans]
           (async/>!! ch :terminate)
           )
         )
       )
     )
   )
  )

;; (def client1 (client/create client/default-uri))

;; (def c (start-heartbeat client1))

;; (async/>!! c "stop")

;; (client/close client1)

;; (defn do-work
;;   [a]
;;   (print (str "Doing " a))
;;   (print "Done")
;;   )


;; (faktory/perform-async do-work 1)
