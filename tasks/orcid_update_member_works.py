import json
import time
import luigi
import pandas as pd

from util.orcid.member import get_orcid_member_works
from util.luigi.orcid_task import OrcidTask
from tasks.orcid_modified_members import OrcidModifiedMembersTask
from util.common import to_snake_case
from util.postgres import query, write_table


class OrcidUpdateMemberWorksTask(OrcidTask):
    """
    Description: OrcidUpdateMemberWorksTask fetches the ORCID member works for the ORCID members that have been modified.
    By default, the task fetches the modified records for the last 7 days, but the date range can be adjusted.
    The task requires the affiliation name as a parameter and depends on the OrcidModifiedMembersTask. The task saves
    the member works to the Postgres table and saves the number of rows written to the local target that is used to
    detect if the task has been executed
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_table_name = 'orcid_modified_member'
        self.pg_target_table_name = 'orcid_member_works'

    # Task input parameters
    affiliation_name: str = luigi.Parameter(
        description='Affiliation, organization name, e.g. "University of Ljubljana"'
    )
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
        return OrcidModifiedMembersTask(affiliation_name=self.affiliation_name,
                                        updated_date_start=self.updated_date_start,
                                        updated_date_end=self.updated_date_end)

    def query_records_to_update(self) -> list:
        """
        Fetch the modified records that will be updated
        :return: List of modified records
        """
        # Escape the single quotes in the affiliation name
        affiliation = self.affiliation_name.replace("'", "''")
        # Fetch the modified records from Postgres
        modified_records_df = query(
            conn=self.pg_connection,
            query=f"SELECT DISTINCT member_id FROM {self.source_table_name} WHERE affiliation = '{affiliation}'"
        )

        modified_records = modified_records_df['member_id'].tolist()
        self.logger.info(f'Modified records found: {len(modified_records)}')
        return modified_records

    def process_item(self, item: str):
        """
        Process the modified records
        :param item: Modified record
        :return: Processed record
        """
        ix = item
        # Enforce the rate limit before each request
        self.rate_limit()

        # Fetch the ORCID record
        record = get_orcid_member_works(member_id=ix, access_token=self.orcid_access_token)

        # Return the record
        return record

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Transform the modified records to a DataFrame
        :param iterable: List of modified records
        :return: Pandas DataFrame with the modified records
        """
        # Convert the list of records to a DataFrame
        df = pd.DataFrame(iterable)
        # Add row creation and last update timestamps. The row created timestamp will only be added, when the row is created.
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        df['row_updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        # Count the number of works for each member
        df['member_works_count'] = df['member_works'].apply(lambda x: len(x))
        # Convert the member_works column to a JSON string
        df['member_works'] = df['member_works'].apply(lambda x: json.dumps(x))

        # Add the task parameters to the DataFrame
        df['task_params_spec'] = self.params_spec

        # Return the DataFrame
        return df

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        affiliation = to_snake_case(self.affiliation_name)
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"{affiliation}_update_member_works_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        OrcidUpdateMemberWorksTask(affiliation_name="Ca' Foscari University of Venice"),
        OrcidUpdateMemberWorksTask(affiliation_name="University of Ljubljana")
    ],
        local_scheduler=True)
