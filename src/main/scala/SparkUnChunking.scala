import java.nio.ByteBuffer
import java.sql.Blob

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import scala.util.Try

case class chunkMetaCaseClass(filename: String, filesize: Long, chunkcount: Long)
case class chunkCaseClass(filename: String, seqnum: Long, bytes: Array[Byte])

object SparkUnChunking {

  def main(args: Array[String]) {

    // Check how many arguments were passed in
    if(args.length == 0)
    {
      System.out.println("Error - no filename supplied")
      System.out.println("Proper Usage is: dse spark-submit --class SparkChunking ./target/scala-2.10/spark-chunking_2.10-0.1.jar <filename>");
      System.exit(0);
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
    // create a new SparkSQLContext
    val csc = new CassandraSQLContext(sc)

    // ========== main code starts here =========
    val file_name: String = args(0) // scala doesn't like args[0]
    //val bigfile = sc.binaryFiles(s"file://" + filePath + fileName)

    csc.setKeyspace(cassandraKeyspace)
    // dereference the first thing in the list
    // it comes back as an array of the columns in that row - which may contain 1 column
    // so 0 is the 0th element
    val chunk_count:BigInt = csc.sql(s"select chunkcount from chunk_meta where filename='1Mfile';").first()(0).asInstanceOf[Long]
    val file_size = csc.sql(s"select filesize from chunk_meta where filename='1Mfile';").first()(0)

    //val chunk: Option[Int] = chunk_count
    println("File name    : " + file_name)
    println("File size    : " + file_size)
    println("32K Chunks   : " + chunk_count)

    var out = None: Option[FileOutputStream]

    var i: BigInt = 1

    try {
      out = Some(new FileOutputStream(file_name + "_copy"))
      while ( {
        i <= chunk_count
      }) {
        val chunk:Array[Byte] = csc.sql(s"select bytes from chunk_data where filename='$file_name' and seqnum=$i;").first()(0).asInstanceOf[Array[Byte]]
        println("Writing chunk " + i + " to " + file_name + "_copy" )
        out.get.write(chunk)
        i=i+1
      }
    } catch {
      case e: IOException => e.printStackTrace
    } finally {
      println("Entered finally ...")
      if (out.isDefined) out.get.close
    }












    System.exit(0)

    sc.stop()
  }
}
