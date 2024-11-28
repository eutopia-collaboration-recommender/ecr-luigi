import threading
import urllib

import luigi

from box import Box
from util.postgres import create_connection
from pymongo import MongoClient

# Global lock for rate limiting (shared across all tasks, thread-safe)
request_lock = threading.Lock()
request_timestamps = []  # Shared across all tasks


class ElsevierTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Read settings from config file
        # Note: The config file should be stored in the same directory where script is executed
        self.config = Box.from_yaml(filename="config.yaml")
        self.num_rows = self.config.ORCID.NUM_ROWS_PER_PAGE

        # MongoDB connection
        mongo_username = urllib.parse.quote_plus(self.config.MONGODB.USERNAME)
        mongo_password = urllib.parse.quote_plus(self.config.MONGODB.PASSWORD)
        mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@{self.config.MONGODB.HOST}:{self.config.MONGODB.PORT}"
        self.mongo_client = MongoClient(mongo_uri)
        # Access the MongoDB database
        self.mongo_db = self.mongo_client[self.config.MONGODB.DATABASE]
        # Set window size for batch processing
        self.mongo_batch_size = self.config.MONGODB.BATCH_SIZE

        # PostgreSQL connection
        self.pg_connection = create_connection(
            username=self.config.POSTGRES.USERNAME,
            password=self.config.POSTGRES.PASSWORD,
            host=self.config.POSTGRES.HOST,
            port=self.config.POSTGRES.PORT,
            database=self.config.POSTGRES.DATABASE,
            schema=self.config.POSTGRES.SCHEMA
        )
