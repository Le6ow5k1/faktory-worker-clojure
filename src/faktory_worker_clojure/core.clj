(ns faktory-worker-clojure.core
  (:require [faktory-worker-clojure.client :as client]
            [faktory-worker-clojure.connection-pool :refer [with-conn shutdown]]
            [clojure.core.async :as async]
            [crypto.random :as random]
            [taoensso.timbre :as timbre]
            )
  (:import [java.util.concurrent Executors TimeUnit])
  )

(def defaults
  {:heartbeat-period-ms 15000
   :queue :default
   :concurrency 10
   :shutdown-timeout-ms 25000})

(def job-fns (atom {}))

(defn try-beat
  [conn-pool]
  (try
    (with-conn conn-pool
      #(client/beat %))
    (catch Throwable e
      (timbre/error e "Error during heartbeat"))))

(defn start-heartbeat
  [client]
  (let [c (async/chan)]
    (async/go
      (loop []
        (let [[stop? _] (async/alts! [c (async/timeout (defaults :heartbeat-period-ms))])]
          (if stop?
            stop?
            (do
              (try-beat client)
              (recur))))))
    c))

(defn try-fetch
  [conn-pool queue]
  (let [ch (async/chan)]
    (try
      (with-conn conn-pool
        #(client/fetch % queue))
      (catch InterruptedException e
        (do
          (timbre/debug "Interrupting job fetching" queue)
          (throw e)))
      (catch Throwable e (timbre/error e "Error fetching job" queue)))))

(defn try-process
  [conn-pool {:keys [jobtype jid args]}]
  (try
    (if-let [job-fn (get @job-fns jobtype)]
      (do
        (timbre/info "Processing job" jobtype jid "with args:" args)
        (apply job-fn args)
        (with-conn conn-pool
          #(client/ack % jid))
        (timbre/debug "Processed job" jobtype jid "with args:" args))
      (throw (Exception. (str "Job " jobtype "isn't registered"))))
    (catch InterruptedException e
      (do
        (timbre/debug "Interrupting job processing" jobtype jid)
        (throw e)))
    (catch Throwable e
      (do (timbre/error e "Error processing job" jobtype jid)
          (with-conn conn-pool
            #(client/fail % jid e))))))

(defn fetch-and-process
  [conn-pool]
  (when-let [job (try-fetch conn-pool (defaults :queue))]
    (try-process conn-pool job)))

(defn run-worker
  [conn-pool]
  (try
    (loop []
      (do
        (fetch-and-process conn-pool)
        (Thread/sleep 1000)
        (recur)))))

(defprotocol WorkerManager
  (start [this])
  (stop [this]))

(defn create-worker-manager
  ([conn-pool] (create-worker-manager conn-pool (defaults :concurrency)))
  ([conn-pool pool-size]
   (let [worker-pool (Executors/newFixedThreadPool pool-size)]
     (reify WorkerManager
       (start [this]
         (let [workers (doall (for [n (range pool-size)]
                                #(run-worker conn-pool)))]
           (timbre/info "Starting" pool-size "workers")
           (doseq [w workers]
             (.submit worker-pool w))))
       (stop [this]
         (try
           (when-not (.awaitTermination worker-pool 1000 TimeUnit/MILLISECONDS)
             (timbre/info "Shutting down workers")
             (.shutdownNow worker-pool))
           (catch InterruptedException e
             (timbre/debug e)
             (timbre/info "Shutting down workers")
             (.shutdownNow worker-pool))
           (finally
             (shutdown conn-pool #(client/close %)))))))))

(defn register-job
  [name fn]
  (swap! job-fns assoc (str name) fn))

(defn perform-async
  "Pushes job into a queue. Job should be registered.

    (perform-async conn-pool ::sum-numbers-job [2, 3])
  "
  [conn-pool name args & {:keys [queue]
                          :as opts
                          :or {queue (defaults :queue)}}]
  (when (not (contains? @job-fns (str name)))
    (let [msg (str "Job "
                   name
                   " not found in registry. You need to register the job using register-job function.")]
      (throw (Exception. msg))))
  (let [job {:jid (random/hex 12)
             :queue queue
             :args args
             :jobtype (str name)}]
    (timbre/info "Pushing job" job)
    (with-conn conn-pool
      #(client/push % job))))

;; (def pool (conn-pool/create {:size 10} #(client/create)))
;; (def mngr (create-worker-manager pool))
;; (start mngr)
;; (stop mngr)
;; (defn job1 [a] (prn a " YAHOOOOO"))
;; (register-job ::job1 job1)
;; (perform-async pool ::job1 ["It works!!!"] {})
;; (shutdown pool #(client/close %))
