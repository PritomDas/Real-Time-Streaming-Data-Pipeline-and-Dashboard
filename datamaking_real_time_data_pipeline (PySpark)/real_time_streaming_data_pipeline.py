from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "server-live-status"
KAFKA_BOOTSTRAP_SERVERS_CONS = '192.168.99.100:9092'
# https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.0.1
# https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.6.0
# https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10-assembly_2.12/3.0.1

# PostgreSQL Database Server Details
postgresql_host_name = "192.168.99.100"
postgresql_port_no = "5432"
postgresql_user_name = "demouser"
postgresql_password = "demouser"
postgresql_database_name = "event_message_db"
postgresql_driver = "org.postgresql.Driver"
# https://mvnrepository.com/artifact/org.postgresql/postgresql/42.2.16
# --packages org.postgresql:postgresql:42.2.16

# https://mvnrepository.com/artifact/org.apache.commons/commons-pool2/2.8.1

#Create the Database properties
db_properties = {}
db_properties['user'] = postgresql_user_name
db_properties['password'] = postgresql_password
db_properties['driver'] = postgresql_driver

def save_to_postgresql_table(current_df, epoc_id, postgresql_table_name):
    print("Inside save_to_postgresql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing postgresql_table_name: " + postgresql_table_name)

    postgresql_jdbc_url = "jdbc:postgresql://" + postgresql_host_name + ":" + str(postgresql_port_no) + "/" + postgresql_database_name

    #Save the dataframe to the table.
    current_df.write.jdbc(url = postgresql_jdbc_url,
                  table = postgresql_table_name,
                  mode = 'append',
                  properties = db_properties)

    print("Exit out of save_to_postgresql_table function")

if __name__ == "__main__":
    print("Real-Time Streaming Data Pipeline Started ...")

    spark = SparkSession \
        .builder \
        .appName("Real-Time Streaming Data Pipeline") \
        .master("local[*]") \
        .config("spark.jars", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar,file:///D://spark_dependency_jars//postgresql-42.2.16.jar,file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar,file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar,file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraClassPath", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraLibrary", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.driver.extraClassPath", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()

    '''spark = SparkSession \
        .builder \
        .appName("Real-Time Streaming Data Pipeline") \
        .master("local[*]") \
        .getOrCreate()'''

    # spark-submit --master local[*] --jars /opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar,/opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar,/opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar,/opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar --conf spark.executor.extraClassPath=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:/opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:/opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar:/opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar --conf spark.executor.extraLibrary=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:/opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:/opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar:/opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar --conf spark.driver.extraClassPath=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:/opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:/opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar:/opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar real_time_streaming_data_pipeline.py

    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("INFO")

    # Construct a streaming DataFrame that reads from server-live-status kafka topic
    event_message_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of event_message_detail_df: ")
    event_message_detail_df.printSchema()

    # Schema Code Block Starts Here
    event_message_detail_schema = StructType([
      StructField("event_id", StringType()),
      StructField("event_server_status_color_name_severity_level", StringType()),
      StructField("event_datetime", StringType()),
      StructField("event_server_type", StringType()),
      StructField("event_country_code", StringType()),
      StructField("event_country_name", StringType()),
      StructField("event_city_name", StringType()),
      StructField("event_estimated_issue_resolution_time", IntegerType()),
      StructField("event_server_status_other_param_1", StringType()),
      StructField("event_server_status_other_param_2", StringType()),
      StructField("event_server_status_other_param_3", StringType()),
      StructField("event_server_status_other_param_4", StringType()),
      StructField("event_server_status_other_param_5", StringType()),
      StructField("event_server_config_other_param_1", StringType()),
      StructField("event_server_config_other_param_2", StringType()),
      StructField("event_server_config_other_param_3", StringType()),
      StructField("event_server_config_other_param_4", StringType()),
      StructField("event_server_config_other_param_5", StringType())
    ])
    # Schema Code Block Ends Here

    event_message_detail_df_1 = event_message_detail_df.selectExpr("CAST(value AS STRING)")

    event_message_detail_df_2 = event_message_detail_df_1.select(from_json(col("value"), event_message_detail_schema).alias("event_message_detail"))

    event_message_detail_df_3 = event_message_detail_df_2.select("event_message_detail.*")

    print("Printing Schema of event_message_detail_df_3: ")
    event_message_detail_df_3.printSchema()

    # Data Processing/Data Transformation
    # 1. Split the column event_server_status_color_name_severity_level to
    # event_server_status_color_name, event_server_status_severity_level
    # 2. Add new column event_message_count with counter 1
    # event_server_status_color_name_severity_level --> Green|Severity 3
    event_message_detail_df_4 = event_message_detail_df_3\
        .withColumn("event_server_status_color_name", split(event_message_detail_df_3["event_server_status_color_name_severity_level"], '\|')[0]) \
        .withColumn("event_server_status_severity_level", split(event_message_detail_df_3["event_server_status_color_name_severity_level"], '\|')[1]) \
        .withColumn("event_message_count", lit(1))

    # Write raw data into HDFS
    #hdfs_event_message_path = "hdfs://192.168.99.100:9000/data/json/event_message_detail_raw_data"
    #hdfs_event_message_checkpoint_location_path = "hdfs://192.168.99.100:9000/data/checkpoint/event_message_detail_raw_data"
    #hdfs_event_message_path = "hdfs://masternode:9000/data/json/event_message_detail_raw_data"
    #hdfs_event_message_checkpoint_location_path = "hdfs://masternode:9000/data/checkpoint/event_message_detail_raw_data"
    #hdfs_event_message_path = "hdfs://192.168.99.100:9000/data/json/event_message_detail_raw_data"
    #hdfs_event_message_checkpoint_location_path = "hdfs://192.168.99.100:9000/data/checkpoint/event_message_detail_raw_data"
    hdfs_event_message_path = "file:///C://docker_workarea//spark_job//data//json//event_message_detail_raw_data"
    hdfs_event_message_checkpoint_location_path = "file:///C://docker_workarea//spark_job//data//checkpoint//event_message_detail_raw_data"

    event_message_detail_df_4.writeStream \
      .trigger(processingTime='20 seconds') \
      .format("json") \
      .option("path", hdfs_event_message_path) \
      .option("checkpointLocation", hdfs_event_message_checkpoint_location_path) \
      .start()

    '''event_message_detail_df_5 = event_message_detail_df_4.select(
      col("event_country_code"),
      col("event_country_name"),
      col("event_city_name"),
      col("event_server_status_color_name"),
      col("event_server_status_severity_level"),
      col("event_estimated_issue_resolution_time"),
      col("event_message_count"))'''

    event_message_detail_df_5 = event_message_detail_df_4.select(["event_country_code", \
      "event_country_name", \
      "event_city_name", \
      "event_server_status_color_name", \
      "event_server_status_severity_level", \
      "event_estimated_issue_resolution_time", \
      "event_message_count"])

    # Data Processing/Data Transformation
    event_message_detail_agg_df = event_message_detail_df_5\
        .groupby("event_country_code",
                 "event_country_name",
                 "event_city_name",
                 "event_server_status_color_name",
                 "event_server_status_severity_level")\
        .agg(fn.sum('event_estimated_issue_resolution_time').alias('total_estimated_resolution_time'),
              fn.count('event_message_count').alias('total_message_count'))

    postgresql_table_name = "event_message_detail_agg_tbl"

    event_message_detail_agg_df \
    .writeStream \
    .trigger(processingTime='20 seconds') \
    .outputMode("update") \
    .foreachBatch(lambda current_df, epoc_id: save_to_postgresql_table(current_df, epoc_id, postgresql_table_name)) \
    .start()

    # Write result dataframe into console for debugging purpose
    event_message_detail_write_stream = event_message_detail_df_4 \
        .writeStream \
        .trigger(processingTime='20 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    event_message_detail_agg_write_stream = event_message_detail_agg_df \
        .writeStream \
        .trigger(processingTime='20 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    event_message_detail_write_stream.awaitTermination()

    print("Real-Time Streaming Data Pipeline Completed.")
