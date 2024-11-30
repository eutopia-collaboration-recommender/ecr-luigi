import json
import time
import luigi
import pandas as pd

from tasks.crossref_update_publications import CrossrefUpdatePublicationsTask
from tasks.elsevier_update_publications import ElsevierUpdatePublicationsTask
from tasks.orcid_update_member_person import OrcidUpdateMemberPersonTask
from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.luigi.eutopia_task import EutopiaTask
from util.orcid.access_token import get_access_token
from util.orcid.member import get_orcid_member_works
from util.common import to_snake_case


class MainUpdateTask(EutopiaTask):
    """
    Description: OrcidUpdateMemberWorksTask fetches the ORCID member works for the ORCID members that have been modified.
    By default, the task fetches the modified records for the last 7 days, but the date range can be adjusted.
    The task requires the affiliation name as a parameter and depends on the OrcidModifiedMembersTask. The task saves
    the member works to the Postgres table and saves the number of rows written to the local target that is used to
    detect if the task has been executed
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )
    updated_date_end: str = luigi.OptionalParameter(description='Search end date', default='NOW')

    def requires(self):
        """
        Requires the OrcidModifiedMembersTask for given affiliation name and date range to be processed before
        this task.
        :return: OrcidModifiedMembersTask
        """
        all_tasks = list()
        # Update Elsevier publications
        all_tasks.append(ElsevierUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                        updated_date_end=self.updated_date_end))

        # Update Crossref publications
        # ** Note that this also triggers fetching ORCID modified members and ORCID member works for all EUTOPIA institutions
        all_tasks.append(CrossrefUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                        updated_date_end=self.updated_date_end))

        # Update ORCID member metadata for all EUTOPIA institutions
        all_tasks.extend(
            [OrcidUpdateMemberPersonTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['INSTITUTION_PRETTY_NAME'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end)
                for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()]
        )
        # Return requirements
        return all_tasks

    def run(self):
        """
        Run the main task. Update ORCID works JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
        self.logger.info(f"Running {self.__class__.__name__} for affiliation: {self.affiliation_name}.")

        # Get the access token

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        affiliation = to_snake_case(self.affiliation_name)
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"main_update_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([MainUpdateTask()], local_scheduler=True)
