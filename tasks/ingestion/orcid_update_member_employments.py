import time
import luigi
import pandas as pd

from util.orcid.member import get_orcid_member_employments
from util.luigi.orcid_task import OrcidTask
from tasks.ingestion.orcid_modified_members import OrcidModifiedMembersTask
from util.common import to_snake_case
from util.postgres import query


class OrcidUpdateMemberEmploymentsTask(OrcidTask):
    """
    Description: OrcidUpdateMemberEmploymentsTask is a Luigi Task that fetches the ORCID member employments for a given
    affiliation name and date range. The task requires the OrcidModifiedMembersTask to be processed before this task.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_table_name = 'orcid_modified_member'
        self.pg_target_table_name = 'orcid_member_employments'

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

        # Query for the modified records
        query_str = f"""
        SELECT DISTINCT member_id 
        FROM {self.source_table_name} 
        WHERE affiliation = '{affiliation}'
            AND member_id NOT IN (SELECT DISTINCT member_id FROM {self.pg_target_table_name})
        """

        # Fetch the modified records from Postgres
        modified_records_df = query(
            conn=self.pg_connection,
            query=query_str
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
        record = get_orcid_member_employments(member_id=ix, access_token=self.orcid_access_token)

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
        target_name = f"{affiliation}_update_member_employments_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        OrcidUpdateMemberEmploymentsTask(affiliation_name="Ca' Foscari University of Venice"),
        OrcidUpdateMemberEmploymentsTask(affiliation_name="University of Ljubljana")
    ],
        local_scheduler=True)
