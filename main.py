import threading
import time
import logging
from kafka_producer import produce_to_kafka
from kafka_consumer import consume_from_kafka
from spark_streaming import start_spark_streaming
from cassandra_connector import CassandraConnector
from config import CASSANDRA_KEYSPACE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def monitor_threads(threads):
    while True:
        for i, thread in enumerate(threads):
            if not thread.is_alive():
                logger.warning(f"{thread.name} stopped. Restarting...")
                new_thread = threading.Thread(target=thread._target, name=thread.name, daemon=True)
                new_thread.start()
                threads[i] = new_thread
        time.sleep(5)

def main():
    cassandra = CassandraConnector()
    cassandra.create_keyspace(CASSANDRA_KEYSPACE)
    cassandra.create_table('financial_data', ['symbol text', 'price float', 'volume int'])

    consumer_thread = threading.Thread(target=consume_from_kafka, name="KafkaConsumer", daemon=True)
    spark_thread = threading.Thread(target=start_spark_streaming, name="SparkStreaming", daemon=True)

    consumer_thread.start()
    spark_thread.start()

    monitor_thread = threading.Thread(target=monitor_threads, args=([consumer_thread, spark_thread]), daemon=True)
    monitor_thread.start()

    try:
        produce_to_kafka("AAPL")
    except Exception as e:
        logger.error(f"Producer error: {e}")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
