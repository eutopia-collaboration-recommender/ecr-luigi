import threading
from util.luigi.eutopia_task import EutopiaTask

# Global lock for rate limiting (shared across all tasks, thread-safe)
request_lock = threading.Lock()
request_timestamps = []  # Shared across all tasks


class CrossrefTask(EutopiaTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_url = 'https://api.crossref.org/works'
        self.params = {'mailto': self.config.CROSSREF.MAILTO}
        # Number of records to checkpoint
        self.num_records_to_checkpoint = self.config.CROSSREF.NUM_RECORDS_TO_CHECKPOINT

