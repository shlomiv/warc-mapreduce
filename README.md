# warc-mapreduce

a working version of warc for hadoop's new api (mapreduce), based on lemur project, with a few fixes (in the java directory)

There's also an example for using warc with hadoop-clojure. To run the example, get a file from common-crawl (first crawl of 2013 http://commoncrawl.org/new-crawl-data-available/ ):

    s3cmd get s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-20/segments/1368710313659/wet/CC-MAIN-20130516131833-00097-ip-10-60-113-184.ec2.internal.warc.wet.gz

and an example for a file from the winter 2013 crawl (http://commoncrawl.org/winter-2013-crawl-data-now-available/), dont forget to change the file name in example.clj test:

    s3cmd get s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1387345775423/wet/CC-MAIN-20131218054935-00092-ip-10-33-133-15.ec2.internal.warc.wet.gz


then run

    lein test warc-mapreduce.example 

 
