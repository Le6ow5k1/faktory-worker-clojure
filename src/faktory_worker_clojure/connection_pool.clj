(ns faktory-worker-clojure.connection-pool
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(def default-opts {:size 5 :timeout-ms 2500})

(defprotocol IConnectionPool
  (checkout [this] "Takes connection from a pool")
  (checkin [this conn] "Returns connection to a pool")
  (with-conn [this f] "Calls function f with connection from pool as an argument")
  (shutdown [this f] "Calls function f with connection as an argument for every connection in pool"))

(defn create
  [given-opts create-fn]
  (let [{:keys [size timeout-ms]} (merge default-opts given-opts)
        connections (LinkedBlockingQueue. size)
        created-count (atom 0)]
    (reify IConnectionPool
      (checkout [this]
        (if (empty? connections)
          (if (< @created-count size)
            (let [conn (create-fn)]
              (swap! created-count inc)
              (.put ^LinkedBlockingQueue connections conn)
              conn)
            (.poll ^LinkedBlockingQueue connections timeout-ms TimeUnit/MILLISECONDS))
          (.poll ^LinkedBlockingQueue connections timeout-ms TimeUnit/MILLISECONDS)))
      (checkin [this conn]
        (.put ^LinkedBlockingQueue connections conn))
      (with-conn [this f]
        (let [conn (checkout this)]
          (try
            (f conn)
            (finally (checkin this conn)))))
      (shutdown [this f]
        (while (not-empty connections)
          (when-let [conn (.poll ^LinkedBlockingQueue connections timeout-ms TimeUnit/MILLISECONDS)]
            (f conn)))))))
