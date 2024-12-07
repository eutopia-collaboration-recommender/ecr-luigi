import json
import time
import luigi

from tasks.dbt.dbt_data_ingestion import DbtDataIngestionTask
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class DataIngestionTask(EutopiaTask):
    """
    Description: Main task to update all the data sources for the EUTOPIA project. The task requires the start and end date:
    1. Elsevier publications.
    2. ORCID members that changed in the given time period (for all EUTOPIA institutions).
    3. ORCID member works for all EUTOPIA institutions.
    4. ORCID member metadata (person JSON) for all EUTOPIA institutions.
    5. Crossref publications for all modified ORCID records in the given time period.
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
        """
        Requires all tasks to be completed before this task can be run.
        :return: OrcidModifiedMembersTask
        """

        # Return requirements
        return DbtDataIngestionTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end)

    def run(self):
        """
        Run the main task. Update ORCID works JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
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
    luigi.build([DataIngestionTask()], local_scheduler=True)
