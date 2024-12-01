import threading
import time
import luigi

from box import Box

from util.luigi.eutopia_task import EutopiaTask
from util.postgres import create_connection

# Global lock for rate limiting (shared across all tasks, thread-safe)
request_lock = threading.Lock()
request_timestamps = []  # Shared across all tasks


class CrossrefTask(EutopiaTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.mailto = self.config.CROSSREF.MAILTO
        self.base_url = 'https://api.crossref.org/works'
        # Number of records to checkpoint
        self.num_records_to_checkpoint = self.config.CROSSREF.NUM_RECORDS_TO_CHECKPOINT