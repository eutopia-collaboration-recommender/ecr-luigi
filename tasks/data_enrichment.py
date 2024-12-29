import json
import time
import luigi

from tasks.dbt.dbt_data_enrichment import DbtDataEnrichmentTask
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class DataEnrichmentTask(EutopiaTask):
    """
    Description: Main task to enrich the data generated in the data ingestion task. The task requires the start and end
    date and runs the following tasks:
    1. Calculate the collaboration novelty index.
    2. Classify article language.
    3. Embed top N articles for research ares (only done once).
    4. Embed articles using specter2.
    5. Classify article research areas.
    6. Generate article keywords using Ollama.
    7. Fetch article keyword trends from Crossref.
    8. Running dbt to update the data enrichment mart.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )
    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time())))

    def requires(self):
        # Return requirements
        return [
            DbtDataEnrichmentTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end)
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
        target_name = f"main_update_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([DbtDataEnrichmentTask()], local_scheduler=True)
