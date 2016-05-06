# Introduction

Cassandra is often considered as a solution to store or manage objects. The sizes and types of objects vary according to the individual use case, and present different challenges that require different solutions, for example:

•	Streaming media services

•	Small object storage

•	Medium to large object storage


The benefits of being able to store binary objects (blobs), in Cassandra are those that would be expected of a fully resilient and scalable distributed database:

•	Scalability

•	Availability

•	Geographic distribution


However, there are questions surrounding the performance and feasibility of storing large objects beyond 10K, and even small files can present problems when the objects are frequently written or updated.

Where the object sizes are of a suitable (limited) size, breaking them into a larger number of smaller component chunks is a very efficient mechanism for distributing the storage requirement across a number of nodes whilst avoiding the risk of overloading Java heap capacity on each node with too many big writes. There are practical limitations to the size of files that would be suitable for chunking but we have seen successful tests with a 3.6Gb file chunked into 20 byte chunks and stored in Cassandra in 20 seconds. 


## Build and run the Test example

1. Build the fat jar file package ```sbt package```
1. Submit the jar to the spark server ```dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar $1 $2 $3```


1. Alternatively from the command line run the shell script called chunk - just provide the file name to chunk, the required chunk size, and whether you want to chunk the file in parallel (recommended for speed) or serial mode. :
```./chunk <file_to_chunk>`<chunk size in bytes> <[p]arallel|[s]erial>``

1. To extract a file from the database as <filename>_copy:
```./unchunk <file_to_unchunk>```

```Example:
$ ./chunk
Error - no filename supplied
Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename> <chunk size in bytes> <[p]arallel|[s]erial>

./chunk fma-oracle.gif 64 s
Parameters are: File name, chunk size, p|s
*****************************************
Writing chunks to Cassandra in serial
Chunk size: 64 bytes
File name : fma-oracle.gif
File exists : true
File size : 243648
64 byte chunks : 3807
Modulo : 0
Total chunks : 3807
*****************************************
Saving blob file metadata to Cassandra....
File fma-oracle.gif metadata saved to Cassandra
fma-oracle.gif, 243648 bytes
Saving binary chunks to Cassandra....
Total bytes saved : 243648
'''
Here's an example recovering a file that was broken into 32K chunks:

$ ./unchunk fma.gif
File name    : fma.gif
File size    : 243648
Chunks       : 8
Writing chunk 1 to fma.gif_copy
Writing chunk 2 to fma.gif_copy
Writing chunk 3 to fma.gif_copy
Writing chunk 4 to fma.gif_copy
Writing chunk 5 to fma.gif_copy
Writing chunk 6 to fma.gif_copy
Writing chunk 7 to fma.gif_copy
Writing chunk 8 to fma.gif_copy
File fma.gif successfully un-chunked

$ diff fma.gif fma.gif_copy
$
