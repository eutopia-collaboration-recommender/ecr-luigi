import json
import time
import luigi
import pandas as pd

from util.orcid.access_token import get_access_token
from util.orcid.member import search_modified_records
from util.luigi.orcid_task import OrcidTask
from util.common import to_snake_case
from util.postgres import write_table


class OrcidModifiedMembersTask(OrcidTask):
    """
    Description: OrcidModifiedMembersTask fetches the ORCID members that have been modified. By the default the task
    fetches the modified records for the last 7 days, but the date range can be adjusted. The task requires the
    affiliation name as a parameter. The task saves the modified records to the Postgres table and saves the number of
    rows written to the local target that is used to detect if the task has been executed.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_table_name = 'orcid_modified_member'

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

    def num_batches(self, access_token) -> int:
        # Fetch the number of modified records
        num_found = search_modified_records(
            access_token=access_token,
            affiliation=self.affiliation_name,
            num_rows=0
        )['num-found']

        # Fetch the number of total batches
        num_batches = num_found // self.num_rows + 1

        return num_batches

    def fetch_modified_records(self, access_token: str, num_batches: int) -> list:
        # Fetch the modified records
        modified_records = list()
        for ix in range(num_batches):
            # Enforce the rate limit before each request
            self.rate_limit()

            # Fetch the modified records
            response = search_modified_records(
                access_token=access_token,
                affiliation=self.affiliation_name,
                start=ix * self.num_rows,
                num_rows=self.num_rows
            )

            # Append the modified records to the list
            modified_records.extend([result['orcid-identifier'] for result in response['result']])
        return modified_records

    def to_dataframe(self, modified_records: list) -> pd.DataFrame:
        # Convert the modified records to a DataFrame
        df = pd.DataFrame(modified_records)
        df.columns = ['url', 'member_id', 'host']
        # Set modify date to the current time
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        # Add affiliation column
        df['affiliation'] = [self.affiliation_name] * len(df)
        return df

    def run(self):
        # Fetch the access token
        access_token = get_access_token(client_id=self.client_id, client_secret=self.client_secret)

        # Fetch the number of total batches
        num_batches = self.num_batches(access_token=access_token)

        # Iterate over the batches and load the modified records
        modified_records = self.fetch_modified_records(access_token=access_token, num_batches=num_batches)

        # Convert the modified records to a DataFrame
        df = self.to_dataframe(modified_records=modified_records)

        # Save the DataFrame to Postgres
        num_rows_written = write_table(conn=self.connection, df=df, table_name=self.target_table_name)

        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'num-rows-written': num_rows_written})
            f.write(f"{result}")

    def output(self):
        affiliation = to_snake_case(self.affiliation_name)
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"{affiliation}_modified_members_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        OrcidModifiedMembersTask(affiliation_name="Ca' Foscari University of Venice"),
        OrcidModifiedMembersTask(affiliation_name="University of Ljubljana")
    ],
        local_scheduler=True)
