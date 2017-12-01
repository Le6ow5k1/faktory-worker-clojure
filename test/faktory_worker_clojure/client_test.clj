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

  (testing "Zero bytes bulk string response"
    (let [reader (make-reader-stub {:read-line-resp "$0\r\n"})]
      (is (= (c/read-and-parse {:reader reader}) nil))))

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
          reader (make-reader-stub {:read-line-resp "+OK\r\n"})
          ]
      (is (= (c/beat {:writer writer :reader reader}) "OK"))))

  (testing "Response with state"
    (let [writer (make-writer-stub "BEAT {\"wid\":null}\r\n")
          reader (make-reader-stub {:read-line-resp "$18\r\n"
                                    :read-resp "{\"state\":\"quiet\"}\r\n"})]
      (is (= (c/beat {:writer writer :reader reader}) "quiet"))))
  )

(deftest fetch-test
  (testing "There is no job"
    (let [writer (make-writer-stub "FETCH [\"queue\"]\r\n")
          reader (make-reader-stub {:read-line-resp "$0\r\n"})]
      (is (= (c/fetch {:writer writer :reader reader} :queue) nil))))

  (testing "There is a job"
    (let [writer (make-writer-stub "FETCH [\"queue\"]\r\n")
          reader (make-reader-stub {:read-line-resp "$36\r\n"
                                    :read-resp "{\"jid\":1,\"jobtype\":\"foo\",\"args\":[1]}\r\n"})]
      (is (= (c/fetch {:writer writer :reader reader} :queue) {:jid 1 :jobtype "foo" :args [1]}))))
  )
