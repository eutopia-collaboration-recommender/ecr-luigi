import json
import time
import luigi

from click.testing import CliRunner
from dbt.cli.main import run

from tasks.source_update.crossref_top_n_research_area_publications import CrossrefTopNResearchAreaPublicationsTask
from tasks.source_update.crossref_update_publications import CrossrefUpdatePublicationsTask
from tasks.source_update.elsevier_update_publications import ElsevierUpdatePublicationsTask
from tasks.source_update.orcid_update_member_person import OrcidUpdateMemberPersonTask
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

    def requires(self):
        """
        Requires all tasks to be completed before this task can be run.
        :return: OrcidModifiedMembersTask
        """
        tasks = list()
        # Get CERIF research areas and top N publications for each research area
        tasks.append(CrossrefTopNResearchAreaPublicationsTask())
        # Update Elsevier publications
        tasks.append(ElsevierUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                    updated_date_end=self.updated_date_end))

        # Update Crossref publications
        # ** Note that this also triggers fetching ORCID modified members and ORCID member works for all EUTOPIA institutions
        tasks.append(CrossrefUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                    updated_date_end=self.updated_date_end))

        # Update ORCID member metadata for all EUTOPIA institutions
        tasks.extend(
            [OrcidUpdateMemberPersonTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['INSTITUTION_PRETTY_NAME'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end)
                for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()]
        )
        # Return requirements
        return tasks

    def run(self):
        """
        Run the main task. Update ORCID works JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
        self.logger.info(f"Running {self.__class__.__name__}.")
        # Execute dbt run (tag: data_ingestion)
        dbt_runner = CliRunner()
        dbt_runner.invoke(run, ['--project-dir', 'dbt', '--select', 'tag:data_ingestion'])
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
