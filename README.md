### To build and run the Test example
1. Build the file ```sbt package```
1. Submit the jar to the spark server ```dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-1.0.jar $1


From the command line:
```./run <file_to_chunk>```

If no file name supplied:
```[DSE 4.6 SparkChunking]$ time ./run

Error - no filename supplied

Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-1.0.jar <filename>
```
