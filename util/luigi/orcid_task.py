import threading
import time
import luigi

from box import Box

from util.luigi.eutopia_task import EutopiaTask
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

        self.REQUEST_LIMIT = 24  # Maximum requests per second
        self.REQUEST_WINDOW = 1  # Time window in seconds

    def rate_limit(self):
        """Implements a global rate limiter to respect the request limits."""
        global request_timestamps
        with request_lock:
            current_time = time.time()

            # Remove timestamps older than the REQUEST_WINDOW
            request_timestamps = [
                t for t in request_timestamps
                if current_time - t < self.REQUEST_WINDOW
            ]

            if len(request_timestamps) >= self.REQUEST_LIMIT:
                # Calculate time to sleep until the next request is allowed
                sleep_time = self.REQUEST_WINDOW - (current_time - request_timestamps[0])
                time.sleep(sleep_time)

            # Record the current request timestamp
            request_timestamps.append(current_time)
