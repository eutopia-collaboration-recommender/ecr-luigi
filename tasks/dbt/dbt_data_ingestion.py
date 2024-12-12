import json
import time
import luigi

from dbt.cli.main import dbtRunner, dbtRunnerResult
from tasks.ingestion.crossref_top_n_research_area_publications import CrossrefTopNResearchAreaPublicationsTask
from tasks.ingestion.crossref_update_publications import CrossrefUpdatePublicationsTask
from tasks.ingestion.elsevier_update_publications import ElsevierUpdatePublicationsTask
from tasks.ingestion.eutopia_institutions import EutopiaInstitutionsTask
from tasks.ingestion.orcid_update_member_person import OrcidUpdateMemberPersonTask

from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class DbtDataIngestionTask(EutopiaTask):
    """
    Description:
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


    def run(self):
        """
        Run the main task. Update ORCID works JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
        self.logger.info(f"Running {self.__class__.__name__}.")
        # Execute dbt run (tag: data_ingestion)
        dbt_runner = dbtRunner()
        cli_args = ['run', '--project-dir', './dbt', '--select', 'tag:data_ingestion']
        res: dbtRunnerResult = dbt_runner.invoke(cli_args)

        # inspect the results
        for r in res.result:
            self.logger.info(f"{r.node.name}: {r.status}")
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
        target_name = f"dbt_data_ingestion_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([DbtDataIngestionTask()], local_scheduler=True)
