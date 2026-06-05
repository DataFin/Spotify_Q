import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9092,kafka-2:9094,kafka-3:9096",
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "listening_events")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "s3a://spotify-checkpoints/streaming_trends_job",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SpotifyStreamingTrendsJob")
        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    return spark


def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka listening_events et retourne les events JSON en string.
    Objectif de ce premier job : valider la lecture Kafka avec un sink console.
    """
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("json_event"),
    )

    return events_df


def main():
    spark = build_spark_session()

    events_df = read_kafka_stream(spark)

    query = (
        events_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()