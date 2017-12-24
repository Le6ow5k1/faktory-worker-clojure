(ns faktory-worker-clojure.client
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [crypto.random :as random]
            )
  (:import java.io.StringWriter
           [java.net Socket URI]))

(def default-uri "tcp://localhost:7419")
(def default-timeout-ms 2500)

(defn create-socket
  [given-uri]
  (let [uri (new URI given-uri)
        host (.getHost uri)
        port (.getPort uri)]
    (doto (Socket. host port)
      (.setSoTimeout default-timeout-ms))))

(defn worker-info
  []
  {
   :wid (random/hex 8)
   :hostname (.getHostName (java.net.InetAddress/getLocalHost))
   :pid (->> (java.lang.management.ManagementFactory/getRuntimeMXBean)
             .getName
             (re-find #"\d+")
             Integer/parseInt)
   :labels [(str "clojure-" (clojure-version))]
   :v 1
   }
  )

(defmacro with-timeout
  [& body]
  `(let [future# (future ~@body)
         result# (deref future# ~default-timeout-ms :timeout)]
     (if (= result# :timeout)
       (throw (java.net.SocketTimeoutException.))
       result#)))

(defn read
  [{:keys [reader]}]
  (with-timeout (.readLine reader)))

(defn write
  [{:keys [writer]} message]
  (with-timeout
    (do
      (print message)
      (.append writer message)
      (.flush writer))))

(defn command-error
  [message]
  (Exception. (str "Command error: " message)))

(defn read-and-parse
  [{:keys [reader] :as client}]
  (let [[signal-char & response] (read client)
        response-str (->> response
                          (apply str)
                          str/trim-newline)]
    (case signal-char
      \+ response-str
      \- (throw (command-error response-str))
      \$ (let [read-count (Integer/parseInt response-str)]
           (when (> read-count 0)
             (let [output (char-array read-count)]
               (with-timeout
                 (.read reader output 0 read-count)
                 ;; Read the remaining newline
                 (.readLine reader)
                 (String. output)))))
      (throw (Exception. (str "Parse error: " signal-char response-str))))))

(defn send-command
  ([client verb]
   (write client (str verb "\r\n")))
  ([client verb data]
   (cond
     (map? data) (write client (str verb " " (json/generate-string data) "\r\n"))
     (sequential? data) (write client (str verb " " (str/join " " (map name data)) "\r\n")))))

(defn beat
  [{:keys [wid] :as client}]
  (send-command client "BEAT" {:wid wid})
  (let [response (read-and-parse client)]
    (if (= response "OK")
      response
      (get (json/parse-string response) "state"))))

(defn push
  [client job]
  (send-command client "PUSH" job)
  (let [response (read-and-parse client)]
    (if (= response "OK")
      (get job :jid)
      (throw (command-error response)))))

(defn fetch
  [client & queues]
  (send-command client "FETCH" queues)
  (let [response (read-and-parse client)]
      (json/parse-string response true)))

(defn flush
  [client]
  (send-command client "FLUSH")
  (let [response (read-and-parse client)]
    (if (= response "OK")
      response
      (throw (command-error response)))))

(defn ack
  [client jid]
  (send-command client "ACK" {:jid jid})
  (let [response (read-and-parse client)]
    (if (= response "OK")
      response
      (throw (command-error response)))))

(defn fail
  [client jid e]
  (send-command client "FAIL" {:message (.getMessage e)
                               :errtype (str (class e))
                               :jid jid
                               :backtrace (map #(.toString %) (.getStackTrace e))})
  (let [response (read-and-parse client)]
    (if (= response "OK")
      response
      (throw (command-error response)))))

(defn open
  [{:keys [writer reader] :as client} info]
  (read client)
  (send-command client "HELLO" info)
  (let [response (read-and-parse client)]
    (if (= response "OK")
      response
      (throw (command-error response)))))

(defn close
  [{:keys [writer reader socket] :as client}]
  (send-command client "END")
  (.close writer)
  (.close reader)
  (.close socket))

(defn create
  [uri]
  (let [socket (create-socket uri)
        writer (io/writer socket)
        reader (io/reader socket)
        info (worker-info)
        client {:writer writer :reader reader :socket socket :wid (info :wid)}]
    (open client info)
    client))
