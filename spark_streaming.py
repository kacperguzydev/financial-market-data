import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from config import CASSANDRA_KEYSPACE, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .getOrCreate()

def start_spark_streaming():
    spark = create_spark_session()

    schema = StructType([
        StructField("current_price", FloatType(), True),
        StructField("high_price", FloatType(), True),
        StructField("low_price", FloatType(), True),
        StructField("open_price", FloatType(), True),
        StructField("previous_close", FloatType(), True),
        StructField("timestamp", StringType(), True)
    ])

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    processed_df = df.selectExpr("CAST(value AS STRING) AS json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*").na.fill(0)

    query = processed_df.writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("table", "financial_data") \
        .start()

    logger.info("Streaming data to Cassandra...")
    query.awaitTermination()
