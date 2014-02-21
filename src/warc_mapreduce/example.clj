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
            [clojure-hadoop.config  :refer [conf]]
            )
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
  "This is our implementation of the Mapper.map method.  The key and
  value arguments are sub-classes of Hadoop's Writable interface, so
  we have to convert them to strings or some other type before we can
  use them.  Likewise, we have to call the Context.collect method with
  objects that are sub-classes of Writable."
  [this key ^WritableWarcRecord warc-value ^MapContext context]
  (let [^WarcRecord record (.getRecord warc-value)
        url    (.getHeaderMetadataItem record "WARC-Target-URI") 
        value  (.getContentUTF8 record)]
    (doseq [word (enumeration-seq (StringTokenizer. (str value)))]
      (.write context (Text. word) (LongWritable. 1)))))

(defn reducer-reduce
  "This is our implementation of the Reducer.reduce method.  The key
  argument is a sub-class of Hadoop's Writable, but 'values' is a Java
  Iterator that returns successive values.  We have to use
  iterator-seq to get a Clojure sequence from the Iterator.

  Beware, however, that Hadoop re-uses a single object for every
  object returned by the Iterator.  So when you get an object from the
  iterator, you must extract its value (as we do here with the 'get'
  method) immediately, before accepting the next value from the
  iterator.  That is, you cannot hang on to past values from the
  iterator."
  [this key values ^ReduceContext context]
  (let [sum (reduce + (map (fn [^LongWritable v] (.get v)) values))]
    (.write context key (LongWritable. sum))))

(defn tool-run
  "This is our implementation of the Tool.run method.  args are the
  command-line arguments as a Java array of strings.  We have to
  create a Job object, set all the MapReduce job parameters, then
  call the JobClient.runJob static method on it.

  This method must return zero on success or Hadoop will report that
  the job failed."
  [^Tool this args]
  (doto (Job.)
    (.setJarByClass (.getClass this))
    (conf :name "wordcount1")
      (conf :output-key "org.apache.hadoop.io.Text")
      (conf :output-value "org.apache.hadoop.io.LongWritable")
      (conf :map "warc_mapreduce.example_mapper")
      (conf :reduce "warc_mapreduce.example_reducer")
      (conf :combine "warc_mapreduce.example_reducer")
      (conf :input-format "edu.cmu.lemurproject.WarcFileInputFormat")
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
