
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import scala.concurrent.{future, blocking, Future, Await, duration}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.util.Random

case class chunkMetaDataCaseClass(filename: String, filesize: Long, chunkcount: Long)
case class chunkDataCaseClass(filename: String, seqnum: Long, bytes: Array[Byte])

object SparkChunking {

  def createSchema(cc: CassandraConnector, keySpaceName: String, tableName1: String, tableName2: String) = {
    cc.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keySpaceName} WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute("CREATE TABLE IF NOT EXISTS " +
        s"${keySpaceName}.${tableName1} (filename text, filesize bigint, chunkcount bigint, " +
        s"primary key( filename ));")
      session.execute("CREATE TABLE IF NOT EXISTS " +
        s"${keySpaceName}.${tableName2} (filename text, seqnum bigint, bytes blob, " +
        s"primary key ((filename, seqnum)));")
    }
  }

  def main(args: Array[String]) {

    val startProgramTime: Long = System.currentTimeMillis

    // Check how many arguments were passed in - must be three
    if (args.length <3) {
      System.out.println("Error - one or more missing parameters")
      System.out.println("Proper Usage is: dse spark-submit --class SparkChunking " +
        "./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename> <chunk size in bytes> <[p]arallel|[s]erial>");
      System.exit(0);
    }

    val file_name: String = args(0) // scala doesn't like args[0]
    val chunk_size: BigInt = args(1).toInt
    val chunk_mode: String = args(2)

    println("")
    System.out.println("*****************************************************");
    if (chunk_mode.equals("p")) {
      System.out.println("Writing " + chunk_size + " byte chunks to Cassandra in parallel"); }
    else if (chunk_mode.equals("s")) {
      System.out.println("Writing " + chunk_size + " byte chunks to Cassandra in serial"); }
    else {
      System.out.println("Invalid chunk pattern: " + chunk_mode + " - must be p or s");
      return;
    }
    System.out.println("*****************************************************");

    if (chunk_size<1 || chunk_size>64000) {
      System.out.println("Invalid data sample rate: " + chunk_size + " must be less than 64000");
      return; }

    /* Set the logger level. Optionally increase value from Level.ERROR to LEVEL.WARN or more verbose yet, LEVEL.INFO */
    Logger.getRootLogger.setLevel(Level.ERROR)

    val sparkMasterHost = "127.0.0.1"
    val cassandraHost = "127.0.0.1"
    val cassandraKeyspace = "chunking"
    val cassandraTable1 = "chunk_metadata"
    val cassandraTable2 = "chunk_blobdata"

    // Tell Spark the address of one Cassandra node:
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cleaner.ttl", "3600")
      //.setMaster("spark://" + sparkMasterHost + ":7077")
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(sparkConf)
    lazy val cc = CassandraConnector(sc.getConf)
    createSchema(cc, cassandraKeyspace, cassandraTable1, cassandraTable2)

    // ========== main code starts here =========
    val fq_file_name = file_name
    //val bigfile = sc.binaryFiles(s"file://" + filePath + fileName)

    val file_exists = new java.io.File(fq_file_name).exists
    val file_size = new java.io.File(file_name).length()
    val chunkQuotient = (file_size / chunk_size).intValue
    val remainder = file_size % chunk_size

    var chunksToWrite: Int = 0
    var chunksWritten: Int = 0
    var totalBytesWritten = 0
    var chunkCounter: Int = 0

    if (remainder > 0)
      { chunksToWrite=chunkQuotient + 1 }
    else
      { chunksToWrite=chunkQuotient}

    println("File name : " + file_name)
    println(" - File exists : " + file_exists)
    println(" - File size : " + file_size)
    println(" - " + chunk_size + " byte chunks : " + chunkQuotient)
    println(" - Modulo : " + remainder)
    println("Total chunks to write : " + chunksToWrite)
    println("")
    System.out.println("*****************************************");

    val startChunkTime: Long = System.currentTimeMillis

    // ------ save meta data ------
    if (chunkQuotient > 0 || remainder > 0) {

      val chunkMetaDataSeq = Seq(new chunkMetaDataCaseClass(file_name, file_size, chunksToWrite))
      val collection = sc.parallelize(chunkMetaDataSeq)
      println("Saving blob file metadata to Cassandra....")
      collection.saveToCassandra(cassandraKeyspace, cassandraTable1, SomeColumns("filename", "filesize", "chunkcount"))
      println(" - " + file_name + " metadata saved to Cassandra")
      // ------ save chunk data ------
      def readBinaryFile(input: InputStream): Array[Byte] = {
        val bos = new ByteArrayOutputStream(65535)
        val bis = new BufferedInputStream(input)
        val buf = new Array[Byte](1024)
        Stream.continually(bis.read(buf))
          .takeWhile(_ != -1)
          .foreach(bos.write(buf, 0, _))
        bos.toByteArray
      }
      val fb = readBinaryFile(new FileInputStream(file_name)) // create byte array
      println(" - %s, %d bytes".format(file_name, fb.size))
      println("Writing binary chunks to Cassandra....")
      val y = fb.grouped(chunk_size.toInt)

      while ( chunkCounter < chunkQuotient ) {
        chunkCounter = chunkCounter + 1
        val z = y.next
        // println("Writing chunk #" + i + ", size " + z.size)
        totalBytesWritten = totalBytesWritten + z.size
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, chunkCounter, z))
        val collection = sc.parallelize(chunkDataSeq)
        
        if (chunk_mode == "s") {                                              // serial - the slow way
          // println("Serial save chunk #" + i + ", size " + z.size)
          collection.saveToCassandra(cassandraKeyspace,
            cassandraTable2, SomeColumns("filename", "seqnum", "bytes"))
          chunksWritten = chunksWritten + 1
        }
        else {
          val writeFuture = Future {                                          // parallel - the fast way
            // println("Parallel save chunk #" + i + ", size " + z.size)
            collection.saveToCassandra(cassandraKeyspace,
              cassandraTable2, SomeColumns("filename", "seqnum", "bytes"))
            chunksWritten = chunksWritten + 1
          }

          //val result = Await.result(writeFuture, 1 second)

        }
      }

      if (remainder > 0) {                                                    // leftovers
        chunkCounter = chunkCounter + 1
        val z = y.next
        //println("Saving chunk #" + i + ", size " + z.size)
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, chunkCounter, z))
        val collection = sc.parallelize(chunkDataSeq)
        collection.saveToCassandra(cassandraKeyspace,
          cassandraTable2,SomeColumns("filename", "seqnum", "bytes"))
        totalBytesWritten = totalBytesWritten + z.size

        chunksWritten = chunksWritten + 1
      }
    } else println (" - Nothing to save.....?")

    // wait for futures to complete
    while ( chunksWritten != chunksToWrite )
      {
        Thread.sleep(500)
        println("Chunks written : " + chunksWritten)
      }

    println(" - Total bytes written : " + totalBytesWritten)
    println(" - Total chunks written : " + chunksWritten)

    sc.stop()

    val endProgramTime: Long = System.currentTimeMillis

    println("")
    println("Human run time : " + (endProgramTime-startProgramTime).toFloat / 1000 + " seconds")
    println("Human database time : " + (endProgramTime-startChunkTime).toFloat / 1000 + " seconds")
    println("")
  }

}
