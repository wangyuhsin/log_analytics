import os
import re
import glob
from datetime import datetime, timedelta
import pandas as pd

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import (
    date_format,
    col,
    regexp_extract,
    udf,
    row_number,
    current_timestamp,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from kafka import KafkaConsumer


def kafka_consumer(kafka_bootstrap_servers, kafka_topic):
    # Define the Kafka source options
    kafka_options = {
        "kafka.bootstrap.servers": kafka_bootstrap_servers,
        "subscribe": kafka_topic,
        "startingOffsets": "earliest",
    }

    # Read the Kafka stream using spark.readStream
    df = spark.readStream.format("kafka").options(**kafka_options).load()

    # Data parsing and extraction with Regular Expressions
    host_pattern = r"(^\S+\.[\S+\.]+\S+)\s"
    ts_pattern = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})]"
    method_uri_protocol_pattern = r"\"(\S+)\s(\S+)\s*(\S*)\""
    status_pattern = r"\s(\d{3})\s"
    content_size_pattern = r"\s(\d+)$"

    # Convert the binary message value to string
    logs_df = df.selectExpr("CAST(value AS STRING)").select(
        current_timestamp().alias("current_time"),
        regexp_extract("value", host_pattern, 1).alias("host"),
        regexp_extract("value", ts_pattern, 1).cast("timestamp").alias("time"),
        regexp_extract("value", method_uri_protocol_pattern, 1).alias("method"),
        regexp_extract("value", method_uri_protocol_pattern, 2).alias("endpoint"),
        regexp_extract("value", method_uri_protocol_pattern, 3).alias("protocol"),
        regexp_extract("value", status_pattern, 1).cast("integer").alias("status"),
        regexp_extract("value", content_size_pattern, 1)
        .cast("integer")
        .alias("content_size"),
    )

    hdfs_output_path = "hdfs://127.0.0.1:9000/output"

    query = (
        logs_df.writeStream.outputMode("append")
        .option("checkpointLocation", os.path.join(hdfs_output_path, "checkpoint"))
        .trigger(processingTime="10 seconds")
        .foreachBatch(batch_transform)
        .start()
    )

    query.awaitTermination()


def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast("integer")).alias(col_name)


def batch_transform(logs_df, id):
    # Specify the HDFS output path
    hdfs_output_path = "hdfs://127.0.0.1:9000/output"
    
    # Content Size Statistics
    content_size_summary_df = logs_df.agg(
        F.min(logs_df["content_size"]).alias("min_content_size"),
        F.max(logs_df["content_size"]).alias("max_content_size"),
        F.mean(logs_df["content_size"]).alias("mean_content_size"),
        F.stddev(logs_df["content_size"]).alias("std_content_size"),
        F.count(logs_df["content_size"]).alias("count_content_size"),
    )
    content_size_summary_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "content_size_summary")
    )

    # Finding Missing Values
    exprs = [count_null(col_name) for col_name in logs_df.columns]
    missing_values_df = logs_df.agg(*exprs)
    missing_values_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "missing_values")
    )

    # HTTP Status Code
    http_status_df = logs_df.groupBy("status").count().sort("status")
    http_status_df.write.mode("append").parquet(os.path.join(hdfs_output_path, "http_status"))

    # Analyzing Frequent Hosts
    frequent_hosts_df = (
        logs_df.groupBy("host").count().sort("count", ascending=False).limit(10)
    )
    frequent_hosts_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "frequent_hosts")
    )

    # Frequent EndPoints
    frequent_endPoints_df = (
        logs_df.groupBy("endpoint").count().sort("count", ascending=False).limit(20)
    )
    frequent_endPoints_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "frequent_endPoints")
    )

    # Top Ten Error Endpoints
    error_endpoints_df = (
        logs_df.filter(logs_df["status"] != 200)
        .groupBy("endpoint")
        .count()
        .sort("count", ascending=False)
        .limit(10)
    )
    error_endpoints_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "error_endpoints")
    )

    # Unique Hosts
    unique_hosts_df = logs_df.select("host").distinct()
    unique_hosts_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "unique_hosts")
    )

    # Unique Daily Hosts
    unique_daily_hosts_df = (
        logs_df.select(logs_df.host, F.dayofmonth("time").alias("day"))
        .dropDuplicates()
        .groupBy("day")
        .count()
        .sort("day")
    )
    unique_daily_hosts_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "unique_daily_hosts")
    )

    # Average Number of Daily Requests per Host
    daily_hosts_df = (
        logs_df.select(logs_df.host, F.dayofmonth("time").alias("day"))
        .dropDuplicates()
        .groupBy("day")
        .count()
        .select(col("day"), col("count").alias("total_hosts"))
    )

    total_daily_reqests_df = (
        logs_df.select(F.dayofmonth("time").alias("day"))
        .groupBy("day")
        .count()
        .select(col("day"), col("count").alias("total_reqs"))
    )

    avg_daily_reqests_per_host_df = (
        total_daily_reqests_df.join(daily_hosts_df, "day")
        .withColumn("avg_reqs", col("total_reqs") / col("total_hosts"))
        .sort("day")
    )
    avg_daily_reqests_per_host_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "avg_daily_reqests_per_host")
    )

    # 404 Response Codes
    not_found_df = logs_df.filter(logs_df["status"] == 404).cache()
    endpoints_404_count_df = (
        not_found_df.groupBy("endpoint").count().sort("count", ascending=False)
    )
    endpoints_404_count_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "endpoints_404_count")
    )

    hosts_404_count_df = (
        not_found_df.groupBy("host").count().sort("count", ascending=False)
    )
    hosts_404_count_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "hosts_404_count_df")
    )

    errors_by_date_sorted_df = (
        not_found_df.groupBy(F.dayofmonth("time").alias("day")).count().sort("day")
    )
    errors_by_date_sorted_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "errors_by_date_sorted")
    )

    hourly_avg_errors_sorted_df = (
        not_found_df.groupBy(F.hour("time").alias("hour")).count().sort("hour")
    )
    hourly_avg_errors_sorted_df.write.mode("append").parquet(
        os.path.join(hdfs_output_path, "hourly_avg_errors_sorted")
    )


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("KafkaConsumerSparkStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Kafka consumer settings
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "log_analytics_2GB"

    kafka_consumer(kafka_bootstrap_servers, kafka_topic)
