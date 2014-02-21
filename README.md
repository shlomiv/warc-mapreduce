# warc-mapreduce

a working version of warc for hadoop's new api (mapreduce), based on lemur project, with a few fixes (in the java directory)

There's also an example for using warc with hadoop-clojure. To run the example, get a file from common-crawl :

    s3cmd get s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-20/segments/1368710313659/wet/CC-MAIN-20130516131833-00097-ip-10-60-113-184.ec2.internal.warc.wet.gz

then run
    lein test warc-mapreduce.example 

 
