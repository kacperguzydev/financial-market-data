import os
import logging

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv('FINNHUB_API_KEY', '')
FINNHUB_URL = 'https://finnhub.io/api/v1'

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'financial_quotes'

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT', '9042')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'financial')

if not API_KEY:
    raise ValueError("Missing FINNHUB_API_KEY environment variable")

logging.info("Configuration loaded successfully.")
