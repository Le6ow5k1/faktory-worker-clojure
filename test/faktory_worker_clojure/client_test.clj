(ns faktory-worker-clojure.client-test
  (:require [faktory-worker-clojure.client :as c]
            [clojure.test :refer :all]))

(defprotocol ReaderStub
  (readLine [this])
  (read [this output offset read-count]))

(defn make-reader-stub
  [{:keys [read-line-resp read-resp]}]
  (reify ReaderStub
    (readLine [this] read-line-resp)
    (read [this output offset read-count]
      (let [resp-array (byte-array (map (comp byte int) read-resp))]
        (java.lang.System/arraycopy resp-array offset output offset read-count)))))

(defprotocol WriterStub
  (append [this msg])
  (flush [this]))

(defn make-writer-stub
  [expected-msg]
  (reify WriterStub
    (append [this msg] (is (= msg expected-msg)))
    (flush [this])))

(deftest read-and-parse-test
  (testing "Successful response"
    (let [reader (make-reader-stub {:read-line-resp "+OK\r\n"})]
      (is (= (c/read-and-parse {:reader reader}) "OK"))))

  (testing "Error response"
    (let [reader (make-reader-stub {:read-line-resp "-Error message\r\n"})]
      (is (thrown-with-msg?
           Exception
           #"Command error: Error message"
           (c/read-and-parse {:reader reader})))))

  (testing "Bulk string response"
    (let [reader (make-reader-stub {:read-line-resp "$6\r\n"
                                    :read-resp "foobar\r\n"})]
      (is (= (c/read-and-parse {:reader reader}) "foobar"))))

  (testing "Unexpected response"
    (let [reader (make-reader-stub {:read-line-resp "Foobar"})]
      (is (thrown-with-msg?
           Exception
           #"Parse error: Foobar"
           (c/read-and-parse {:reader reader})))))
  )

(deftest beat-test
  (testing "Successful response"
    (let [writer (make-writer-stub "BEAT {\"wid\":null}\r\n")
          reader (make-reader-stub {:read-line-resp"+OK\r\n"})
          ]
      (is (= (c/beat {:writer writer :reader reader}) "OK"))))

  (testing "Response with state"
    (let [writer (make-writer-stub "BEAT {\"wid\":null}\r\n")
          reader (make-reader-stub {:read-line-resp "$18\r\n"
                                    :read-resp "{\"state\":\"quiet\"}\r\n"})]
      (is (= (c/beat {:writer writer :reader reader}) "quiet"))))
  )
