(ns faktory-worker-clojure.client
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [crypto.random :as random]
            [taoensso.timbre :as timbre])
  (:import java.io.StringWriter
           [java.net Socket URI]
           [javax.net.ssl SSLSocketFactory]
           [java.security MessageDigest]))

(def default-uri "tcp://localhost:7419")
(def default-timeout-ms 2500)

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
   :v 2
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

(defn hash-password
  [iterations password salt]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (doall
     (loop [i iterations
            hash (.getBytes (str password salt))]
       (if (= i 0)
         (.toString (BigInteger. 1 hash) 16)
         (recur (dec i) (.digest digest hash)))))))

(defn open
  [{:keys [writer reader uri] :as client} info]
  (let [[_ password-info] (re-find #"HI (.*)" (read client))]
    (if password-info
      (let [{version :v salt :s iterations :i :or {:i 1}} (json/parse-string password-info true)]
        (when (> version 2)
          (timbre/warn (str "Warning: Faktory server protocol "
                            version
                            " in use, this worker doesn't speak that version.")))
        (when (< iterations 1)
          (throw (Exception. "Invalid hashing")))
        (if salt
          (let [user-info (.getUserInfo uri)
                [_ password] (str/split user-info #":")]
            (if password
              (let [hashed-password (hash-password iterations password salt)]
                (send-command client "HELLO" (assoc info :pwdhash hashed-password)))
              (throw (Exception. "Server requires password, but none has been configured"))))
          (send-command client "HELLO" info)))
      (send-command client "HELLO" info))
    (let [response (read-and-parse client)]
      (if (= response "OK")
        response
        (throw (command-error response))))))

(defn close
  [{:keys [writer reader socket] :as client}]
  (when (not (.isClosed socket))
    (send-command client "END")
    (.close writer)
    (.close reader)
    (.close socket)))

(defn create-ssl-socket
  [host port]
  (let [factory (SSLSocketFactory/getDefault)]
    (doto (.createSocket factory host port))))

(defn create-socket
  [uri]
  (let [host (.getHost uri)
        port (.getPort uri)
        tls? (str/includes? (.getScheme uri) "tls")
        socket (if tls?
                 (create-ssl-socket host port)
                 (Socket. host port))]
    (doto socket
      (.setSoTimeout default-timeout-ms))))

(defn create
  ([] (create default-uri))
  ([given-uri]
   (let [uri (new URI given-uri)
         socket (create-socket uri)
         writer (io/writer socket)
         reader (io/reader socket)
         info (worker-info)
         client {:writer writer :reader reader :socket socket :wid (info :wid) :uri uri}]
     (open client info)
     client)))
