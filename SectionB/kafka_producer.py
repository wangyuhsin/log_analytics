from kafka import KafkaProducer


def kafka_producer(log_file_path, kafka_topic):
    # Set up the Kafka producer
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    # Open the log file
    with open(log_file_path, "r") as file:
        for line in file:
            # Produce the batch to Kafka
            producer.send(kafka_topic, "".join(line).encode("utf-8"))
            producer.flush()


if __name__ == "__main__":
    log_file_path = "2GBFIle.log"
    kafka_topic = "log_analytics_2GB"

    kafka_producer(log_file_path, kafka_topic)
