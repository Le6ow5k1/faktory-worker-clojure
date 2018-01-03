(ns faktory-worker-clojure.client-test
  (:require [faktory-worker-clojure.client :as c]
            [clojure.test :refer :all]
            [faktory-worker-clojure.test-utils :refer :all]))

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
          reader (make-reader-stub {:read-line-resp "+OK\r\n"})]
      (is (= (c/beat {:writer writer :reader reader}) "OK"))))

  (testing "Response with state"
    (let [writer (make-writer-stub "BEAT {\"wid\":null}\r\n")
          reader (make-reader-stub {:read-line-resp "$18\r\n"
                                    :read-resp "{\"state\":\"quiet\"}\r\n"})]
      (is (= (c/beat {:writer writer :reader reader}) "quiet"))))
  )

(deftest fetch-test
  (testing "There is no job"
    (let [writer (make-writer-stub "FETCH queue1 queue2\r\n")
          reader (make-reader-stub {:read-line-resp "$0\r\n"})]
      (is (= (c/fetch {:writer writer :reader reader} :queue1 :queue2)
             nil))))

  (testing "There is a job"
    (let [writer (make-writer-stub "FETCH queue1 queue2\r\n")
          reader (make-reader-stub {:read-line-resp "$36\r\n"
                                    :read-resp "{\"jid\":1,\"jobtype\":\"foo\",\"args\":[1]}\r\n"})]
      (is (= (c/fetch {:writer writer :reader reader} :queue1 :queue2)
             {:jid 1 :jobtype "foo" :args [1]}))))
  )

(deftest fail-test
  (testing "Successful response"
    (let [writer (make-writer-stub
                  "FAIL {\"message\":\"foo\",\"errtype\":\"class java.lang.Exception\",\"jid\":1,\"backtrace\":[\"Class.method(file:1)\"]}\r\n")
          reader (make-reader-stub {:read-line-resp "+OK\r\n"})
          trace-elem (StackTraceElement. "Class" "method" "file" 1)
          trace (into-array StackTraceElement [trace-elem])
          e (Exception. "foo")]
      (.setStackTrace e trace)
      (is (= (c/fail {:writer writer :reader reader} 1 e) "OK"))))
  )
