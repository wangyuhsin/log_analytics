# Log Analytics

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This repository contains two Python scripts for log analytics: `kafka_producer.py` and `spark_stream.py`. These scripts are designed to work together to process log data using Apache Kafka and Apache Spark.

To initiate the process, we need to feed the data, which is an input file, into the Kafka producer. This can be achieved through various approaches, one such approach is injecting the data line by line (feel free to come up with other ideas). This producer is responsible for publishing the data to a specific topic to which it is subscribed.

After the data is injected, the Kafka consumer, which is also subscribed to the same topic, will periodically check for new data. In this case, it will check for available data every 10 seconds.

Once the data is received by the consumer, we will utilize Spark to perform transformations and conduct exploratory data analysis (EDA), employing techniques similar to those discussed in the article.

Finally, the results (e.g., dataframes or RDDs from final transformation) obtained from each analysis will be converted into the Parquet file format, and then these Parquet files will be stored in HDFS.

Flow:
Log File -> Kafka (Producer) -> Spark with Kafka (Consumer which performs transformation using Spark) -> Create Parquet files -> Store the Parquet file in HDFS

## Kafka Producer

The `kafka_producer.py` script reads a log file and produces log entries to a Kafka topic. It uses the `KafkaProducer` class from the `kafka` library to connect to a Kafka broker and send log entries.

### Prerequisites

Before running the `kafka_producer.py` script, make sure you have the following:

- Kafka broker running on `localhost:9092`.
- Log file path specified in the `log_file_path` variable.
- Kafka topic name specified in the `kafka_topic` variable.

### Usage

To run the Kafka producer script, execute the following command:

```shell
python kafka_producer.py
```

In this section, we will be working with a combination of Big Data technologies, including Kafka (producer and consumer), Spark Streaming, Parquet files, and HDFS File System. The objective is to process and analyze log files by establishing a flow that begins with a Kafka producer, followed by a Kafka consumer that performs transformations using Spark. The transformed data will then be converted into Parquet file format and stored in the HDFS.

## Spark Streaming

The `spark_stream.py` script reads log entries from a Kafka topic and performs various analytics on the log data using Apache Spark Streaming.

### Prerequisites

Before running the `spark_stream.py` script, ensure you have the following:

- Apache Spark installed and configured.
- Kafka broker running on `localhost:9092`.
- Kafka topic name specified in the `kafka_topic` variable.
- HDFS (Hadoop Distributed File System) configured and running on `hdfs://127.0.0.1:9000`.
```shell
$HADOOP_HOME/sbin/start-all.sh
```

### Usage

To run the Spark Streaming script, execute the following command:

```shell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark_stream.py
```

Please note that you may need to adjust the Spark package version (`spark-sql-kafka-0-10_2.12:3.4.0`) based on your Spark installation.

## Analytics

The `spark_stream.py` script performs several analytics on the log data received from Kafka and writes the results to Parquet files in the HDFS output path specified in the script. Here are some of the analytics performed:

- Content size statistics
- Missing values analysis
- HTTP status code distribution
- Frequent hosts


- Frequent endpoints
- Top ten error endpoints
- Unique hosts
- Unique daily hosts
- Average number of daily requests per host
- 404 response codes analysis

The results of these analytics are written to Parquet files in the HDFS output path specified in the script.

## Jupyter Notebook: SectionB_demo.ipynb

The SectionB_demo.ipynb notebook demonstrates the results of the log analytics performed using the Kafka producer and Spark Streaming. It provides a step-by-step walkthrough of the analytics process and presents visualizations and analysis of the log data.

To run the notebook, ensure you have Jupyter Notebook installed and execute the following command:
```shell
jupyter notebook SectionB_demo.ipynb
```

## Reference

This project is based on the article "Scalable Log Analytics with Apache Spark: A Comprehensive Case Study" by *Dipanjan (DJ) Sarkar*. You can find the article [here](https://towardsdatascience.com/scalable-log-analytics-with-apache-spark-a-comprehensive-case-study-2be3eb3be977).

## License

This project is licensed under the [MIT License](LICENSE).
