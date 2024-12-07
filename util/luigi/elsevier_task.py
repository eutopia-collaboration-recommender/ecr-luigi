import threading
import urllib

from util.luigi.eutopia_task import EutopiaTask
from pymongo import MongoClient

# Global lock for rate limiting (shared across all tasks, thread-safe)
request_lock = threading.Lock()
request_timestamps = []  # Shared across all tasks


class ElsevierTask(EutopiaTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # MongoDB connection
        mongo_username = urllib.parse.quote_plus(self.config.MONGODB.USERNAME)
        mongo_password = urllib.parse.quote_plus(self.config.MONGODB.PASSWORD)
        mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@{self.config.MONGODB.HOST}:{self.config.MONGODB.PORT}"
        self.mongo_client = MongoClient(mongo_uri)
        # Access the MongoDB database
        self.mongo_db = self.mongo_client[self.config.MONGODB.DATABASE]
        # Set window size for batch processing
        self.mongo_batch_size = self.config.MONGODB.BATCH_SIZE
        # Number of records to checkpoint
        self.num_records_to_checkpoint = self.config.CROSSREF.NUM_RECORDS_TO_CHECKPOINT


    def close_connection(self):
        self.mongo_client.close()
        self.pg_connection.close()
