(ns faktory-worker-clojure.core-test
  (:require [clojure.test :refer :all]
            [shrubbery.core :refer :all]
            [faktory-worker-clojure.core :refer :all]
            [faktory-worker-clojure.test-utils :refer :all]))

(deftest perform-async-test
  (testing "Job isn't registered"
    (let [client (spy (make-fake-client))
          pool (make-conn-pool-stub client)]
      (is (thrown-with-msg?
           Exception
           #"Job .+\/foo not found in registry"
           (perform-async pool ::foo [1])))
      (is (not (received? client push)))))

  (testing "Queue name isn't specified"
    (register-job ::bar #())

    (with-redefs [faktory-worker-clojure.client/push (fn [client job] job)]
      (let [client (spy (make-fake-client))
            pool (make-conn-pool-stub client)
            result (perform-async pool ::bar [1])]
        (is (= (select-keys result [:queue :args :jobtype])
               {:queue :default, :args [1], :jobtype (str ::bar)})))))

  (testing "Queue name is specified"
    (register-job ::bar #())

    (with-redefs [faktory-worker-clojure.client/push (fn [client job] job)]
      (let [client (spy (make-fake-client))
            pool (make-conn-pool-stub client)
            result (perform-async pool ::bar [1] :queue :bar-queue)]
        (is (= (select-keys result [:queue :args :jobtype])
               {:queue :bar-queue, :args [1], :jobtype (str ::bar)})))))
  )
