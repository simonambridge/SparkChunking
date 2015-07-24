import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
//import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs

import scalax.io._
import java.io._
/* --- */

object SparkChunking {


  def createSchema(cc:CassandraConnector, keySpaceName:String, tableName1:String, tableName2:String) = {
    cc.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keySpaceName} WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${tableName1} (test_id text, chunk_count int, primary key( test_id ));")

      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${tableName2} (test_id text, filename text, seqnum int, " +
                      s"bytes blob, primary key ((test_id, filename, seqnum)));")
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
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[10]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(conf)
    lazy val cc = CassandraConnector(sc.getConf)

    createSchema(cc, cassandraKeyspace, cassandraTable1, cassandraTable2)

    val filePath = "/home/dse/Chunking/"
    val fileName = "100Kfile"
    val fqFileName = filePath + fileName
    //val bigfile = sc.binaryFiles(s"file://" + filePath + fileName)

    //Java
    val fileExists = new java.io.File(fqFileName).exists   
    val fileSize = new java.io.File(fileName).length()

    val chunkCount = (fileSize / 32768.0).intValue

    println("File path    : " + filePath)
    println("File name    : " + fileName)
    println("File exists  : " + fileExists)
    println("File size    : " + fileSize)
    println("Chunk count  : " + chunkCount)
    
    // Ryan:
    // save fileName and chunkCount to table chunk_meta
    // next challenge will be to save the chunks but I should really try that myself first!!!


    //val meta = Map(fileName -> chunkCount) 
    //meta.map(F => (F(0),F(1))).saveToCassandra("benchmark","chunk_meta",SomeColumns("fileName","chunk_count"))
    //case class fileMeta (filename: String, chunks: Int)
    
 
  }

}
