import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

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

    createSchema(cc, cassandraKeyspace, cassandraTable1, cassandraTable2)

    // ========== main code starts here =========

    //val file_path = "/home/dse/"
    //val file_name = "100Kfile"
    val file_name: String = args(0) // scala doesn't like args[0]
    val fq_file_name = file_name
    //val bigfile = sc.binaryFiles(s"file://" + filePath + fileName)

    //Java
    val file_exists = new java.io.File(fq_file_name).exists
    val file_size = new java.io.File(file_name).length()

    val chunk_count = (file_size / 32768.0).intValue
    val remainder = file_size % 32768

    //println("File path    : " + file_path)
    println("File name    : " + file_name)
    println("File exists  : " + file_exists)
    println("File size    : " + file_size)
    println("32K Chunks   : " + chunk_count)
    println("Modulo       : " + remainder)

    // ------ save meta data ------
    if (chunk_count > 0 || remainder > 0) {
      val chunkMetaDataSeq = Seq(new chunkMetaDataCaseClass(file_name, file_size, chunk_count))
      val collection = sc.parallelize(chunkMetaDataSeq)
      println("Saving blob file metadata to Cassandra....")
      collection.saveToCassandra("benchmark", "chunk_meta", SomeColumns("filename", "filesize", "chunkcount"))

      println("File " + file_name + " metadata saved to Cassandra")

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

      val x = fb.grouped(32768).toArray // single element array
      // scala> println(x(0).size)
      // 32768
      // scala> println(x(1).size)
      // 32768
      // scala> println(x(2).size)
      // 32768
      // scala> println(x(chunk_count).size)
      // 4096
      // Total is 102400
      // println(x(22).size)
      // java.lang.ArrayIndexOutOfBoundsException: 22
      // scala> println("%s, %d bytes".format(file_name, fb.size))
      // 100Kfile, 102400 bytes

      var i: Int = 1

      // while ({i <= chunk_count}) {
      //       println(x(i).size)
      //       i=i+1
      //     }
      // 32768
      // 32768
      // 32768
      // 4096

      // scala> fb.grouped(32768).toArray.zipWithIndex foreach(e => println(e._2, e._1.size))
      // returns a tuple of value and index
      // (0,32768)
      // (1,32768)
      // (2,32768)
      // (3,4096)

      println("Saving binary chunks to Cassandra....")
      val y = fb.grouped(32768)
      var totalSize = 0
      while ( {
        i <= chunk_count
      }) {
        val z = y.next
        println("Saving chunk #" + i + ", size " + z.size)
        totalSize = totalSize + z.size
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, i, z))
        val collection = sc.parallelize(chunkDataSeq)
        collection.saveToCassandra("benchmark", "chunk_data", SomeColumns("filename", "seqnum", "bytes"))
        i = i + 1
      }
      if (remainder > 0) {
        val z = y.next
        println("Saving chunk #" + i + ", size " + z.size)
        val chunkDataSeq = Seq(new chunkDataCaseClass(file_name, i, z))
        val collection = sc.parallelize(chunkDataSeq)
        collection.saveToCassandra("benchmark", "chunk_data", SomeColumns("filename", "seqnum", "bytes"))
        totalSize = totalSize + z.size
      }
      println("Total chunks saved : " + totalSize)
      //fb.grouped(32768).map( chunk_tuple => (file_name,chunk_tuple._2, chunk_tuple._1 )).flatMap( x=>x )
      //saveToCassandra("benchmark","chunk_data",SomeColumns("filename","seqnum","bytes"))

    }
    sc.stop()
  }
}
