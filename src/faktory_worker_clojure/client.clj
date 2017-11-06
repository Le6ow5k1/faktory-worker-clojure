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

(defn create-socket
  [given-uri]
  (let [uri (new URI given-uri)
        host (.getHost uri)
        port (.getPort uri)]
    (Socket. host port)))

(defn- worker-info
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

(defn read
  [{:keys [reader]}]
  (.readLine reader))

(defn write
  [{:keys [writer]} message]
  (do
    (print message)
    (.append writer message)
    (.flush writer)))

(defn send-command
  [client verb data]
  (let [data-str (json/generate-string data)]
    (write client (str verb " " data-str "\r\n"))))

(defn beat
  [{:keys [wid] :as client}]
  (send-command client "BEAT" {:wid wid})
  (read client))

(defn create-client
  [uri]
  (let [socket (create-socket uri)
        writer (io/writer socket)
        reader (io/reader socket)
        info (worker-info)
        client {:writer writer :reader reader :wid (info :wid)}]
    (print (read client))
    (send-command client "HELLO" info)
    (print (read client))
    client))

(def client (create-client default-uri))

(beat client)

(read client)

(send-command client "INFO" {})
