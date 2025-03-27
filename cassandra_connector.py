import logging
from cassandra.cluster import Cluster
from config import CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self):
        self.cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
        self.session = self.cluster.connect()
        self.create_keyspace(CASSANDRA_KEYSPACE)
        self.session.set_keyspace(CASSANDRA_KEYSPACE)

    def close(self):
        self.session.shutdown()
        self.cluster.shutdown()
        logger.info("Cassandra connection closed.")

    def create_keyspace(self, keyspace):
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
        """
        self.session.execute(query)

    def create_table(self, table, columns):
        query = f"CREATE TABLE IF NOT EXISTS {table} (id UUID PRIMARY KEY, {', '.join(columns)})"
        self.session.execute(query)
