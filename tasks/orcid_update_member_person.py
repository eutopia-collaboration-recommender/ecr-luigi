import json
import time
import luigi
import pandas as pd

from util.orcid.access_token import get_access_token
from util.orcid.member import get_orcid_member_person
from util.luigi.orcid_task import OrcidTask
from tasks.orcid_modified_members import OrcidModifiedMembersTask
from util.common import to_snake_case
from util.postgres import query, write_table


class OrcidUpdateMemberPersonTask(OrcidTask):
    """
    Description: OrcidUpdateMemberWorksTask fetches the ORCID member works for the ORCID members that have been modified.
    By the default the task fetches the modified records for the last 7 days, but the date range can be adjusted.
    The task requires the affiliation name as a parameter and depends on the OrcidModifiedMembersTask. The task saves
    the member works to the Postgres table and saves the number of rows written to the local target that is used to
    detect if the task has been executed
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_table_name = 'orcid_modified_member'
        self.pg_target_table_name = 'orcid_member_person'

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

    def fetch_modified_records(self) -> list:
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

    def fetch_member_person(self, modified_records: list, access_token: str) -> list:
        """
        Fetch the ORCID member person JSON for the given modified records
        :param modified_records: List of modified records
        :param access_token: ORCID API access token
        :return: List of ORCID member works
        """
        member_person_records = list()
        for ix, member_id in enumerate(modified_records):
            # Enforce the rate limit before each request
            self.rate_limit()
            # Fetch the ORCID record
            record = get_orcid_member_person(member_id=member_id, access_token=access_token)
            # Append the record to the list
            member_person_records.append(record)

            if ix % 50 == 0:
                self.logger.info(f"Processed {ix} records")

        return member_person_records

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
        return df

    def run(self):
        """
        Run the main task. Update ORCID person JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
        self.logger.info(f"Running {self.__class__.__name__} for affiliation: {self.affiliation_name}.")

        # Fetch the access token
        access_token = get_access_token(client_id=self.client_id, client_secret=self.client_secret)

        # Fetch the modified records that will be updated
        modified_records = self.fetch_modified_records()

        # Fetch the member works
        member_person_records = self.fetch_member_person(modified_records=modified_records, access_token=access_token)

        # Write the modified records to the PostgreSQL database and save the number of rows written to the local target
        self.on_run_finished(iterable=member_person_records)

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        affiliation = to_snake_case(self.affiliation_name)
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"{affiliation}_update_member_person_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        OrcidUpdateMemberPersonTask(affiliation_name="Ca' Foscari University of Venice"),
        OrcidUpdateMemberPersonTask(affiliation_name="University of Ljubljana")
    ],
        local_scheduler=True)
