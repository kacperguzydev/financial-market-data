import logging
import json
from kafka import KafkaConsumer
from config import KAFKA_BROKER, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_message(message):
    logger.info(f"Processing: {message}")

def consume_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )

    for message in consumer:
        process_message(message.value)
        consumer.commit()
