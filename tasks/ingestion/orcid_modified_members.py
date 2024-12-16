import time
import luigi
import pandas as pd

from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.orcid.member import search_modified_records
from util.luigi.orcid_task import OrcidTask
from util.common import to_snake_case


class OrcidModifiedMembersTask(OrcidTask):
    """
    Description: OrcidModifiedMembersTask fetches the ORCID members that have been modified. By the default the task
    fetches the modified records for the last 7 days, but the date range can be adjusted. The task requires the
    affiliation name as a parameter. The task saves the modified records to the Postgres table and saves the number of
    rows written to the local target that is used to detect if the task has been executed.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pg_target_table_name = 'orcid_modified_member'

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

    def query_records_to_update(self) -> int:
        """
        Fetch the number of batches for the modified records
        :return: Number of batches
        """
        # Fetch the number of modified records
        num_found = search_modified_records(
            access_token=self.orcid_access_token,
            affiliation=self.affiliation_name,
            num_rows=0
        )['num-found']

        # Fetch the number of total batches
        num_batches = num_found // self.num_rows + 1
        self.logger.info(f'Number of batches: {num_batches}')

        # Return the number of batches
        return list(range(num_batches))

    def process_item(self, item: int) -> list:
        """
        Process the modified records
        :param item: Modified record
        :return: Processed record
        """
        ix = item
        # Enforce the rate limit before each request
        self.rate_limit()

        # Fetch the modified records
        response = search_modified_records(
            access_token=self.orcid_access_token,
            affiliation=self.affiliation_name,
            start=ix * self.num_rows,
            num_rows=self.num_rows
        )
        if response['result'] is None:
            return []
        # Return the modified records
        return [result['orcid-identifier'] for result in response['result']]

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Transform the modified records to a DataFrame
        :param iterable: List of modified records
        :return: Pandas DataFrame with the modified records
        """
        # Convert the modified records to a DataFrame
        df = pd.DataFrame(iterable)
        df.columns = ['url', 'member_id', 'host']
        # Set modify date to the current time
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        # Add affiliation column
        df['affiliation'] = [self.affiliation_name] * len(df)

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
        target_name = f"{affiliation}_modified_members_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        OrcidModifiedMembersTask(
            affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['institution_pretty_name']
        )
        for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()
    ], local_scheduler=True)
