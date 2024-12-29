import json
import time
import luigi

from tasks.data_enrichment import DataEnrichmentTask
from tasks.data_ingestion import DataIngestionTask
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class RefreshTask(EutopiaTask):
    """
    Description: data refresh task is a wrapper task that triggers the data ingestion and data enrichment tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    period: str = luigi.OptionalParameter(
        description='Period to ingest data, can be "week" or "month"',
    )

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )
    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time())))

    def requires(self):
        """
        Requires all tasks to be completed before this task can be run.
        :return: List of tasks
        """
        if self.period == 'week':
            self.updated_date_start = time.strftime("%Y-%m-%d",
                                                    time.gmtime(time.time() - 7 * 24 * 60 * 60))
            self.updated_date_end = time.strftime("%Y-%m-%d", time.gmtime(time.time()))
        elif self.period == 'month':
            self.updated_date_start = time.strftime("%Y-%m-%d",
                                                    time.gmtime(time.time() - 30 * 24 * 60 * 60))
            self.updated_date_end = time.strftime("%Y-%m-%d", time.gmtime(time.time()))
        # Return requirements
        return [
            DataIngestionTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end),
            DataEnrichmentTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end)
        ]

    def run(self):
        self.logger.info(f"Running {self.__class__.__name__}.")

        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'task-finished': True})
            f.write(f"{result}")

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"refresh_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([RefreshTask()], local_scheduler=True)
