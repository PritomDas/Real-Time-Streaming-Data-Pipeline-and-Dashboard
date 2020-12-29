name := "datamaking_streaming_data_pipeline"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10-assembly
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10-assembly
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "3.0.1"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.16"
