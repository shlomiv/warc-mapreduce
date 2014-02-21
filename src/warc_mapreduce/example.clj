;; A modified example based on the first example of clojure-hadoop, 
;; that uses warc input files
;;
;; to run:
;;   lein test warc-mapreduce.example  
;;
;; This will count the instances of each word in README.txt and write
;; the results to out1/part-00000

(ns warc-mapreduce.example
  (:require [clojure-hadoop.gen     :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.config  :refer [conf]])
  (:import (java.util StringTokenizer)
           (org.apache.hadoop.util Tool)
           org.apache.hadoop.conf.Configuration
           [edu.cmu.lemurproject WritableWarcRecord WarcFileInputFormat WarcRecord])
  (:use clojure.test))

(imp/import-fs)
(imp/import-io)
(imp/import-mapreduce)
(imp/import-mapreduce-lib)
(gen/gen-job-classes)  ;; generates Tool, Mapper, and Reducer classes
(gen/gen-main-method)  ;; generates Tool.main method

(defn mapper-map
  [this key ^WritableWarcRecord warc-value ^MapContext context]
  ;; extract warc-wet record
  (let [^WarcRecord record (.getRecord warc-value)
        url    (.getHeaderMetadataItem record "WARC-Target-URI") 
        value  (.getContentUTF8 record)]
    (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
      (.write context (Text. word) (LongWritable. 1)))))

(defn reducer-reduce
  [this key values ^ReduceContext context]
  (let [sum (reduce + (map (fn [^LongWritable v] (.get v)) values))]
    (.write context key (LongWritable. sum))))

(defn tool-run
  [^Tool this args]
  (doto (Job.)
    (.setJarByClass (.getClass this))
    (conf :name "wordcount1")
      (conf :output-key "org.apache.hadoop.io.Text")
      (conf :output-value "org.apache.hadoop.io.LongWritable")
      (conf :map "warc_mapreduce.example_mapper")
      (conf :reduce "warc_mapreduce.example_reducer")
      (conf :combine "warc_mapreduce.example_reducer")
      (conf :input-format "edu.cmu.lemurproject.WarcFileInputFormat") ;; use warc
      (conf :output-format "text")
      (conf :compress-output false)
      (conf :input (first args))
      (conf :output (second args))
      (.waitForCompletion false)
      )
  0)


(deftest test-wordcount-1
  (.delete (FileSystem/get (Configuration.)) (Path. "tmp/out1") true)
  (is (tool-run (clojure_hadoop.job.) ["CC-MAIN-20130516131833-00097-ip-10-60-113-184.ec2.internal.warc.wet.gz" "tmp/out1"])))
