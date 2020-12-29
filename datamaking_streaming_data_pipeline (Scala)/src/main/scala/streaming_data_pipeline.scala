package com.datamaking

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object streaming_data_pipeline {
  def main(args: Array[String]): Unit = {
    println("Real-Time Streaming Data Pipeline Started ...")

    // Code Block 1 Starts Here
    // Kafka Broker/Cluster Details
    val KAFKA_TOPIC_NAME_CONS = "server-live-status"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "192.168.99.100:9092"
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.0.1
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.6.0
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10-assembly_2.12/3.0.1

    // PostgreSQL Database Server Details
    val postgresql_host_name = "192.168.99.100"
    val postgresql_port_no = "5432"
    val postgresql_user_name = "demouser"
    val postgresql_password = "demouser"
    val postgresql_database_name = "event_message_db"
    val postgresql_driver = "org.postgresql.Driver"
    // https://mvnrepository.com/artifact/org.postgresql/postgresql/42.2.16
    // --packages org.postgresql:postgresql:42.2.16
    // Code Block 1 Ends Here

    // Code Block 2 Starts Here
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Real-Time Streaming Data Pipeline")
      .getOrCreate()

    System.setProperty("HADOOP_USER_NAME","hadoop")

    spark.sparkContext.setLogLevel("ERROR")

    // Code Block 2 Ends Here

    // Code Block 3 Starts Here
    // Server Status Event Message Data from Kafka
    val event_message_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of event_message_detail_df: ")
    event_message_detail_df.printSchema()
    // Code Block 3 Ends Here

    // Code Block 4 Starts Here
    val event_message_detail_schema = StructType(Array(
      StructField("event_id", StringType),
    StructField("event_server_status_color_name_severity_level", StringType),
    StructField("event_datetime", StringType),
    StructField("event_server_type", StringType),
    StructField("event_country_code", StringType),
    StructField("event_country_name", StringType),
    StructField("event_city_name", StringType),
    StructField("event_estimated_issue_resolution_time", IntegerType),
    StructField("event_server_status_other_param_1", StringType),
    StructField("event_server_status_other_param_2", StringType),
    StructField("event_server_status_other_param_3", StringType),
    StructField("event_server_status_other_param_4", StringType),
    StructField("event_server_status_other_param_5", StringType),
    StructField("event_server_config_other_param_1", StringType),
    StructField("event_server_config_other_param_2", StringType),
    StructField("event_server_config_other_param_3", StringType),
    StructField("event_server_config_other_param_4", StringType),
    StructField("event_server_config_other_param_5", StringType)
    ))
    // Code Block 4 Ends Here

    // Code Block 5 Starts Here
    val event_message_detail_df_1 = event_message_detail_df.selectExpr("CAST(value AS STRING)")

    val event_message_detail_df_2 = event_message_detail_df_1.select(from_json(col("value"), event_message_detail_schema).alias("event_message_detail"))

    val event_message_detail_df_3 = event_message_detail_df_2.select("event_message_detail.*")

    print("Printing Schema of event_message_detail_df_3: ")
    event_message_detail_df_3.printSchema()

    // Data Processing/Data Transformation
    // 1. Split the column event_server_status_color_name_severity_level to
    // event_server_status_color_name, event_server_status_severity_level
    // 2. Add new column event_message_count with counter 1
    // Green|Severity 1
    import org.apache.spark.sql.functions.split
    val event_message_detail_df_4 = event_message_detail_df_3
    .withColumn("event_server_status_color_name", split(col("event_server_status_color_name_severity_level"), "\\|").getItem(0))
    .withColumn("event_server_status_severity_level", split(col("event_server_status_color_name_severity_level"), "\\|").getItem(1))
    .withColumn("event_message_count", lit(1))

    // Write raw data into HDFS
    val local_storage_path = "file:///C://docker_workarea//spark_job//data//json//event_message_detail_raw_data"
    val local_storage_checkpoint_location_path = "file:///C://docker_workarea//spark_job//data//checkpoint//event_message_detail_raw_data"
    event_message_detail_df_4.writeStream
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .format("json")
    .option("path", local_storage_path)
    .option("checkpointLocation", local_storage_checkpoint_location_path)
    .start()

    // Code Block 5 Ends Here

    // Code Block 6 Ends Here
    val event_message_detail_df_5 = event_message_detail_df_4.selectExpr("event_country_code",
    "event_country_name",
    "event_city_name",
    "event_server_status_color_name",
    "event_server_status_severity_level",
    "event_estimated_issue_resolution_time",
    "event_message_count")

    // Data Processing/Data Transformation
    val event_message_detail_agg_df = event_message_detail_df_5.groupBy("event_country_code",
      "event_country_name",
      "event_city_name",
      "event_server_status_color_name",
      "event_server_status_severity_level")
    .agg(sum("event_estimated_issue_resolution_time").alias("total_estimated_resolution_time"),
    count("event_message_count").alias("total_message_count"))

    val postgresql_table_name = "dashboard_event_message_detail_agg_tbl"
    val postgresql_jdbc_url = "jdbc:postgresql://" + postgresql_host_name + ":" + postgresql_port_no.toString() + "/" + postgresql_database_name

    println("postgresql_jdbc_url: " + postgresql_jdbc_url)
    println("postgresql_table_name: " + postgresql_table_name)

    // Create the Database properties
    val db_connection_properties = new Properties()
    db_connection_properties.put("user", postgresql_user_name)
    db_connection_properties.put("password", postgresql_password)
    db_connection_properties.put("url", postgresql_jdbc_url)
    db_connection_properties.put("driver", postgresql_driver)

    event_message_detail_agg_df.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database/table

        batchDF.write.mode("append").jdbc(postgresql_jdbc_url, postgresql_table_name, db_connection_properties)

        /*// Save the dataframe to the table.
        batchDF.write
          .format("jdbc")
          .option("url", postgresql_jdbc_url)
          .option("driver", postgresql_driver)
          .option("dbtable", postgresql_table_name)
          .option("user", postgresql_user_name)
          .option("password", postgresql_password)
          .option("mode", "append")
          .save()*/
      }.start()

    // Code Block 6 Starts Here

    // Code Block 7 Starts Here
    // Write result dataframe into console for debugging purpose
    val event_message_detail_write_stream = event_message_detail_df_4
    .writeStream
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .outputMode("update")
    .option("truncate", "false")
    .format("console")
    .start()

    val event_message_detail_agg_write_stream = event_message_detail_agg_df
      .writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    event_message_detail_write_stream.awaitTermination()

    println("Real-Time Streaming Data Pipeline Completed.")
    // Code Block 7 Ends Here
  }
}
