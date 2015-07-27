import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs

import scalax.io._
import java.io._
/* --- */

 import scala.Predef._ 
 import org.apache.spark.streaming.StreamingContext._ 
 import scala.language.implicitConversions
 import org.apache.spark.sql._

/* --- */

 case class chunkMetaDataCaseClass (fileid: Int, filename: String, filesize: Long, chunkcount: Long)
 case class chunkDataCaseClass (fileid: Int, filename: String, seqnum: Long, bytes: Array[Byte])

object SparkChunking {


  def createSchema(cc:CassandraConnector, keySpaceName:String, tableName1:String, tableName2:String) = {
    cc.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keySpaceName} WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${tableName1} (fileid int, filename text, filesize bigint, chunkcount bigint, " +
                      s"primary key( fileid ));")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${tableName2} (fileid int, filename text, seqnum bigint, bytes blob, " +
                      s"primary key ((fileid, filename, seqnum)));")
    }
  }


  /* Set the logger level. Optionally increase value from Level.ERROR to LEVEL.WARN or more verbose yet, LEVEL.INFO */
  Logger.getRootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val sparkMasterHost = "127.0.0.1"
    val cassandraHost = "127.0.0.1"
    val cassandraKeyspace = "benchmark"
    val cassandraTable1 = "chunk_meta"
    val cassandraTable2 = "chunk_data"

    // Tell Spark the address of one Cassandra node:
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[10]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(sparkConf)
    lazy val cc = CassandraConnector(sc.getConf)

    createSchema(cc, cassandraKeyspace, cassandraTable1, cassandraTable2)

// ========== main code starts here =========

    val file_path = "/home/dse/SparkChunking/"
    val file_name = "100Kfile"
    val fq_file_name = file_path + file_name
    //val bigfile = sc.binaryFiles(s"file://" + filePath + fileName)

    //Java
    val file_exists = new java.io.File(fq_file_name).exists   
    val file_size = new java.io.File(file_name).length()

    val chunk_count = (file_size / 32768.0).intValue
    val file_id = 1

    println("File path    : " + file_path)
    println("File name    : " + file_name)
    println("File exists  : " + file_exists)
    println("File size    : " + file_size)
    println("Chunk count  : " + chunk_count)
    println("File ID      : " + file_id)
 
    // ------ save meta data ------
    val chunkMetaDataSeq = Seq(new chunkMetaDataCaseClass(file_id, file_name, file_size, chunk_count))
    val collection = sc.parallelize(chunkMetaDataSeq)
    collection.saveToCassandra("benchmark","chunk_meta",SomeColumns("fileid","filename","filesize","chunkcount"))


  }
}
