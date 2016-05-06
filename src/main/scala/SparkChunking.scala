
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import scala.concurrent.{future, blocking, Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.concurrent._
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
        s"primary key ((filename), seqnum));")
    }
  }
  def main(args: Array[String]) {
    // Check how many arguments were passed in
    if (args.length <3) {
      System.out.println("Usage: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename> <chunksize (bytes)> <[p]arallel|[s]erial>");
      System.out.println("Error - parameter errors")
      System.out.println("Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename>");
      System.exit(0);
    }

    val file_name: String = args(0) // scala doesn't like args[0]
    val chunk_size: BigInt = args(1).toInt
    val chunk_mode: String = args(2)

    System.out.println("*****************************************");
    if (chunk_mode.equals("p")) {
      System.out.println("Writing " + chunk_size + " byte chunks to Cassandra in parallel"); }
    else if (chunk_mode.equals("s")) {
      System.out.println("Writing chunks to Cassandra in serial"); }
    else {
      System.out.println("Invalid chunk pattern: " + chunk_mode);
      return;
    }

    if (chunk_size<0 || chunk_size>64500) {
      System.out.println("Invalid data sample rate: " + chunk_size);
      return; }
    else {
      System.out.println("Chunk size: " + chunk_size + " bytes");
    }

    /* Set the logger level. Optionally increase value from Level.ERROR to LEVEL.WARN or more verbose yet, LEVEL.INFO */
    Logger.getRootLogger.setLevel(Level.ERROR)

    val sparkMasterHost = "127.0.0.1"
    val cassandraHost = "127.0.0.1"
    val cassandraKeyspace = "benchmark"
    val cassandraTable1 = "chunk_meta"
    val cassandraTable2 = "chunk_data"

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
    val chunk_count = (file_size / chunk_size).intValue
    val remainder = file_size % chunk_size
    var total_chunks = chunk_count
    if (remainder > 0) {total_chunks=chunk_count + 1}
    //println("File path : " + file_path)
    println("File name : " + file_name)
    println("File exists : " + file_exists)
    println("File size : " + file_size)
    println(chunk_size + " byte chunks : " + chunk_count)
    println("Modulo : " + remainder)
    println("Total chunks : " + total_chunks)
    System.out.println("*****************************************");

    // ------ save meta data ------
    if (chunk_count > 0 || remainder > 0) {
      val chunkMetaDataSeq = Seq(new chunkMetaDataCaseClass(file_name, file_size, total_chunks))
      val collection = sc.parallelize(chunkMetaDataSeq)
      println("Saving blob file metadata to Cassandra....")
      collection.saveToCassandra(cassandraKeyspace, cassandraTable1, SomeColumns("filename", "filesize", "chunkcount"))
      println(file_name + " metadata saved to Cassandra")
      // ------ save chunk data ------
      def readBinaryFile(input: InputStream): Array[Byte] = {
        val fos = new ByteArrayOutputStream(65535)
        val bis = new BufferedInputStream(input)
        val buf = new Array[Byte](1024)
        Stream.continually(bis.read(buf))
          .takeWhile(_ != -1)
          .foreach(fos.write(buf, 0, _))
        fos.toByteArray
      }
      val fb = readBinaryFile(new FileInputStream(file_name)) // create byte array
      println("%s, %d bytes".format(file_name, fb.size))
      var i: Int = 1
      println("Saving binary chunks to Cassandra....")
      val y = fb.grouped(chunk_size.toInt)
      var totalSize = 0

      while ( { i <= chunk_count }) {
        val z = y.next
//        println("Saving chunk #" + i + ", size " + z.size)
        totalSize = totalSize + z.size
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, i, z))
        val collection = sc.parallelize(chunkDataSeq)
        if (chunk_mode == "s") {
          //println("Serial save chunk #" + i + ", size " + z.size)
          collection.saveToCassandra(cassandraKeyspace,
            cassandraTable2, SomeColumns("filename", "seqnum", "bytes"))
        }
        else {
          val future = Future {
            //println("Parallel save chunk #" + i + ", size " + z.size)
            collection.saveToCassandra(cassandraKeyspace,
            cassandraTable2, SomeColumns("filename", "seqnum", "bytes"))
          }
          //val result = Await.result(future, 1 second)
        }
        i = i + 1
      }

      if (remainder > 0) {
        val z = y.next
        //println("Saving chunk #" + i + ", size " + z.size)
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, i, z))
        val collection = sc.parallelize(chunkDataSeq)
        collection.saveToCassandra(cassandraKeyspace,
          cassandraTable2,
          SomeColumns("filename", "seqnum", "bytes"))
        totalSize = totalSize + z.size
      }
      println("Total bytes saved : " + totalSize)
    } else println ("Nothing to save.....?")
    sc.stop()
  }
}
