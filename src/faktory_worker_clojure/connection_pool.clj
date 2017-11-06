(ns faktory-worker-clojure.connection-pool)

(def default-opts {:size 5 :timeout-ms 5000})

(def connections (atom []))

(defprotocol IConnectionPool
  (checkout [this] "Takes connection from a pool")
  (checkin [this conn] "Returns connection to a pool")
  (with-conn [this f] "Calls function f with connection from pool as an argument"))

(defn create
  [given-opts create-fn]
  (let [{:keys [size timeout-ms]} (merge default-opts given-opts)
        connections (atom clojure.lang.PersistentQueue/EMPTY)
        ]
    (reify IConnectionPool
      (checkout [this]
        (if (empty? @connections)
          (let [conn (create-fn)]
            (swap! connections conj conn)
            conn)
          (let [conn (first @connections)]
            (swap! connections pop)
            conn)))
      (checkin [this conn]
        (swap! connections conj conn))
      (with-conn [this f]
        (let [conn (checkout this)]
          (try
            (f conn)
            (finally (checkin this conn)))))
      )
    )
  )

(def pool1 (create {} (fn [] (rand-int 10)) ))

(with-conn pool1 identity)
