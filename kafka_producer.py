import logging
import time
import requests
import json
from kafka import KafkaProducer
from config import API_KEY, KAFKA_BROKER, KAFKA_TOPIC, FINNHUB_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FINNHUB_QUOTE_URL = f'{FINNHUB_URL}/quote'

def fetch_stock_quote(symbol, retries=5):
    url = f'{FINNHUB_QUOTE_URL}?symbol={symbol}&token={API_KEY}'
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed ({attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)
    return None

def produce_to_kafka(symbol):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        data = fetch_stock_quote(symbol)
        if data:
            producer.send(KAFKA_TOPIC, data)
            logger.info(f"Sent: {data}")
        time.sleep(10)
