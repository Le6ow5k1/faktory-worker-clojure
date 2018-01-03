(ns faktory-worker-clojure.test-utils
  (:require  [clojure.test :refer :all]
             [faktory-worker-clojure.connection-pool :refer :all]))

(defprotocol ReaderStub
  (readLine [this])
  (read [this output offset read-count]))

(defn make-reader-stub
  [{:keys [read-line-resp read-resp]}]
  (reify ReaderStub
    (readLine [this] read-line-resp)
    (read [this output offset read-count]
      (let [resp-array (char-array read-resp)]
        (java.lang.System/arraycopy resp-array offset output offset read-count)))))

(defprotocol WriterStub
  (append [this msg])
  (flush [this]))

(defn make-writer-stub
  [expected-msg]
  (reify WriterStub
    (append [this msg] (is (= msg expected-msg)))
    (flush [this])))

(defprotocol Client
  (push [this job]))

(defn make-fake-client
  []
  (reify Client
    (push [this job])))

(defn make-conn-pool-stub
  [conn]
  (reify IConnectionPool
    (checkout [this])
    (checkin [this conn])
    (with-conn [this f] (f conn))
    (shutdown [this f])))
