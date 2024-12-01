import threading
import time
import luigi

from box import Box

from util.luigi.eutopia_task import EutopiaTask
from util.orcid.access_token import get_access_token
from util.postgres import create_connection

# Global lock for rate limiting (shared across all tasks, thread-safe)
request_lock = threading.Lock()
request_timestamps = []  # Shared across all tasks


class OrcidTask(EutopiaTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Read settings from config file
        # Note: The config file should be stored in the same directory where script is executed
        self.client_id = self.config.ORCID.CLIENT_ID
        self.client_secret = self.config.ORCID.CLIENT_SECRET
        self.num_rows = self.config.ORCID.NUM_ROWS_PER_PAGE

        self.request_limit = 12  # Maximum requests per second (it's actually 24 requests per second, but we are conservative)
        self.request_window = 1  # Time window in seconds

        self.orcid_access_token = get_access_token(client_id=self.client_id, client_secret=self.client_secret)

        # Number of records to checkpoint
        self.num_records_to_checkpoint = self.config.CROSSREF.NUM_RECORDS_TO_CHECKPOINT

    def rate_limit(self):
        """Implements a global rate limiter to respect the request limits."""
        global request_timestamps
        with request_lock:
            current_time = time.time()

            # Remove timestamps older than the request_window
            request_timestamps = [
                t for t in request_timestamps
                if current_time - t < self.request_window
            ]

            if len(request_timestamps) >= self.request_limit:
                # Calculate time to sleep until the next request is allowed
                sleep_time = self.request_window - (current_time - request_timestamps[0])
                time.sleep(sleep_time)

            # Record the current request timestamp
            request_timestamps.append(current_time)
