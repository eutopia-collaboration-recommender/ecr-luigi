import json
import time
import backoff
import luigi
import requests

from tasks.orcid_update_member_works import OrcidUpdateMemberWorksTask
from util.luigi.crossref_task import CrossrefTask
from util.common import to_snake_case
from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.postgres import query


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=8,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def crossref_request(url: str, params: dict = dict()) -> dict:
    """
    Make a request to the given URL with the given parameters
    :param url: URL to make the request to.
    :param params: Parameters to include in the request.
    :return: JSON response from the request.
    """
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_json = response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        response_json = {}

    return response_json


class CrossrefUpdatePublicationsTask(CrossrefTask):
    """
    Description:
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'crossref_publication'

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )

    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time()))
    )

    def requires(self):
        return [
            OrcidUpdateMemberWorksTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['INSTITUTION_PRETTY_NAME'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end
            )
            for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()
        ]

    def query_dois(self) -> list:
        """
        Query the DOIs from the PostgreSQL database
        :return: List of DOIs
        """
        doi_query = f"""
            SELECT DISTINCT eid ->> 'external-id-value' AS publication_doi
            FROM orcid_member_works mw,
                 LATERAL JSONB_ARRAY_ELEMENTS(mw.member_works) AS w,
                 LATERAL JSONB_ARRAY_ELEMENTS(w -> 'external-ids' -> 'external-id') AS eid
            WHERE eid ->> 'external-id-type' = 'doi'
              AND eid ->> 'external-id-value' NOT IN (SELECT publication_doi
                                                     FROM crossref_publication)
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=doi_query)
        return df['publication_doi'].tolist()

    def process_dois(self, dois: list) -> list:
        """
        Process the DOIs and fetch the publication metadata
        :param dois: List of DOIs
        :return: List of publication metadata
        """
        # Initialize the results list
        publications = list()
        # Iterate over the DOIs and fetch the publication metadata
        for ix, doi in enumerate(dois):
            publication_metadata = crossref_request(url=f'{self.base_url}/{doi}')
            record = dict(
                publication_doi=doi,
                publication_metadata=json.dumps(publication_metadata)
            )

            publications.append(record)

            # Print progress
            if ix % 50 == 0:
                print(f"Processed {ix} records")

        # Return the results
        return publications

    def run(self):
        """
        Run the main task. Process Elsevier publications. Write the results to the PostgreSQL database and save the
        number of rows written to a local target file.
        """
        dois = self.query_dois()

        # Process the DOIs and fetch the publication metadata
        publications = self.process_dois(dois)

        # Write the modified records to the PostgreSQL database and save the number of rows written to the local target
        self.on_run_finished(iterable=publications)

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"crossref_publications_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        CrossrefUpdatePublicationsTask(),
    ], local_scheduler=True)
