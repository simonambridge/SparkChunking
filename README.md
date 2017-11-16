# Cassandra As An Object Store

I frequently encounter clients and prospective customers who enquire whether it is feasible to use Cassandra as an object store. 

There are questions surrounding the performance and feasibility of storing objects beyond even relatively small sizes (even a few MB) - and even smaller files may still present problems when the objects are frequently written or updated.

On the other hand, where files must be uploaded and stored, there can be performance advantages in breaking a file into smaller fragments and uploading the fragments to storage in parallel, instead of uploading the file as a single contiguous object.

The sizes and types of candidate objects vary according to the individual use cases. Different use cases present differing challenges,  and while some solutions will require careful sizing and design, others might be completely unsuitable for use with Cassandra.

There are a variety of use cases where it is convenient to store regular files of a more manageable size – e.g. text files, documents, thumbnail images etc - anything from 5 or 10K up to larger sizes of, say, 60MB. As with most things Cassandra, the answer has an element of “it depends”, meaning that Cassandra’s suitability depends upon the particular use case. 
The danger with working with larger data elements is that storing them in Cassandra has to be done carefully in order to avoid causing excessive heap pressure and potential hot spots.

- Cassandra’s storage engine requires that all the data for a single key must fit on disk on a single machine (node) in the cluster - storing large files could therefore easily lead to hotspots on a certain nodes.
- Reads and writes of data in Cassandra use the Java Heap, meaning that if you read 500 rows - each containing a column storing a 10Mb image file - the result set would consume about 5Gb of RAM. RAM consumption would increase rapidly as more rows/columns are loaded. Depending on the ‘concurrency * size’ relationship particular to a use case this may or may not be a problem, and with some careful design and testing it would be possible to produce a workable design for most use cases.
- From an implementation standpoint if there is a lot of blob data it could also push the node count up significantly.

Historically Netflix's Astyanax library provided a mechanism for storing (large) binary objects in Cassandra via utility classes that addressed the problem by splitting up large objects into multiple keys and distributing them randomly in order to avoid creation of hotspots. However Astyanax was a Thift-based driver and in February 2016 Netflix announced its retirement (see Appendix C for an example of doing this with Spark).

Let us also not forget that there are many existing solutions already built on Cassandra that successfully store data as blobs - and the Cassandra Wiki (https://wiki.apache.org/cassandra/CassandraLimitations) says “single digits of MB" is a more reasonable limit, since there is no streaming or random access of blob values”. 

It’s not so much a matter of ‘can it be done’, more a question of  ‘should it be done for this particular use case’, and if so, how best to approach the solution. 

One approach would be to break the files up into smaller chunks and store those chunks distributed evenly across the cluster using the in-built distributed storage feature of Cassandra. An ideal mechanism for achieving this might be to use a parallelised Scala or Java process as described here. This would avoid the problem of node storage hotspots when storing files, and heap pressure on a single node when writing or reading a file.

# Prequisites
## Install sbt (Scala Build Tool)
```
echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list 
sudo apt-get update 
sudo apt-get install sbt
```

## 1	Build The Jar Package

sbt package



## 2	Submit The Jar To The Spark Server
Parameters are: the file to chunk, the size of the chunks, and whether to write the chunks to Cassandra in parallel or in series:

```
dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar $1 $2 $3
```


## 3	From The Command Line
Use the supplied shell script ('chunk') for convenience:

```
./chunk <file_to_chunk> <chunk size in bytes> <[p]arallel|[s]erial>
```
For example - chunk the file 'image_file.png' into 512 byte chunks, in parallel:
```
./chunk image_file.png 512 p
```

```
$ ./chunk

Parameters are: File name, chunk size, p|s
Error - one or more missing parameters
Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename> <chunk size in bytes> <[p]arallel|[s]erial>
```

```
$ ./chunk fma-oracle.gif 1024 p
Parameters are: File name, chunk size, p|s

*****************************************************
Writing 1024 byte chunks to Cassandra in parallel
*****************************************************
File name : fma-oracle.gif
 - File exists : true
 - File size : 243648
 - 1024 byte chunks : 237
 - Modulo : 960
Total chunks to write : 238

*****************************************
Saving blob file metadata to Cassandra....
 - fma-oracle.gif metadata saved to Cassandra                                   
 - fma-oracle.gif, 243648 bytes
Writing binary chunks to Cassandra.
.............................................................................................................................................................................................................................................
39 of 238 futures written...
64 of 238 futures written...
90 of 238 futures written...
115 of 238 futures written...
142 of 238 futures written...
166 of 238 futures written...
193 of 238 futures written...
220 of 238 futures written...
238 of 238 futures written...
 - Total bytes written : 243648
 - Total chunks written : 238

Human run time : 14.744 seconds
Human database time : 7.096 seconds

```

```
$ diff fma.gif fma.gif_copy

$
```



