name := "Spark Chunking"

version := "0.1"

scalaVersion := "2.10.5"

organization := "datastax.com"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.1" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  
libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0"
