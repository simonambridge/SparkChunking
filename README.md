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

Where the object sizes are of a suitable (limited) size, breaking them into a larger number of sufficiently smaller component chunks is a very efficient mechanism for distributing the storage requirement across a number of nodes whilst avoiding the risk of overloading the Java heap capacity on each node. There are practical limitations to the size of files that would be suitable for chunking but we have seen successful tests with a 3.6Gb file chunked into 20 byte chunks and stored in Cassandra in 20 seconds. 


## Build and run the Test example
1. Build the file ```sbt package```
1. Submit the jar to the spark server ```dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar $1```


1. From the command line:
```./chunk <file_to_chunk>```

1. To extract a file from the database as <filename>_copy:
```./unchunk <file_to_unchunk>```

```Example:
$ ./chunk
Error - no filename supplied
Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename>

$ ./chunk fma.gif
File name    : fma.gif
File exists  : true
File size    : 243648
32K Chunks   : 7
Modulo       : 14272
Total chunks : 8
Saving blob file metadata to Cassandra....
File fma.gif metadata saved to Cassandra
fma.gif, 243648 bytes
Saving binary chunks to Cassandra....
Saving chunk #1, size 32768
Saving chunk #2, size 32768
Saving chunk #3, size 32768
Saving chunk #4, size 32768
Saving chunk #5, size 32768
Saving chunk #6, size 32768
Saving chunk #7, size 32768
Saving chunk #8, size 14272
Total chunks saved : 243648

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
