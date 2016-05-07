#1	Build the fat jar file package:

sbt package



#2	Submit the jar to the spark server:

```
dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar $1 $2 $3
```


#3	From the command line

```
./chunk <file_to_chunk> <chunk size in bytes> <[p]arallel|[s]erial>
```

```
$ ./chunk

Error - no filename supplied

Proper Usage is: dse spark-submit --class SparkChunking

./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename> <chunk size in bytes> <[p]arallel|[s]erial>
```

```
$ ./chunk fma.gif 8196 p

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
```


#4 To extract a file from the database with “_copy” appended to filename

```
./unchunk <file_to_unchunk>
```

```
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
```

```
$ diff fma.gif fma.gif_copy

$
```



