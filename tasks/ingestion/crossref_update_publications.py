import json
import time
import luigi

from tasks.ingestion.orcid_update_member_works import OrcidUpdateMemberWorksTask
from util.crossref.works import crossref_request
from util.luigi.crossref_task import CrossrefTask
from util.common import to_snake_case
from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.postgres import query


class CrossrefUpdatePublicationsTask(CrossrefTask):
    """
    Description: A Luigi task to fetch the publication metadata from Crossref for the DOIs in the PostgreSQL database
    from ORCID member works. By default, the task fetches the publication metadata for the DOIs that have been updated
    in the last 7 days. The task requires the OrcidUpdateMemberWorksTask to be completed before it can be run.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'crossref_publication'
        self.delete_insert = False

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
        """
        The task requires the OrcidUpdateMemberWorksTask to be completed before it can be run.
        :return: List of all OrcidUpdateMemberWorksTask
        """
        return [
            OrcidUpdateMemberWorksTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['institution_pretty_name'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end
            )
            for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()
        ]

    def query_records_to_update(self) -> list:
        """
        Query the DOIs from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
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
                   query=query_str)
        return df['publication_doi'].tolist()

    def process_item(self, item: str) -> dict:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        doi = item
        # Fetch the publication metadata
        publication_metadata = crossref_request(url=f'{self.base_url}/{doi}', params=self.params)
        # Define the record JSON
        record = dict(
            publication_doi=doi,
            publication_metadata=json.dumps(publication_metadata)
        )

        # Return the record
        return record

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
        CrossrefUpdatePublicationsTask(updated_date_start='2024-01-01', updated_date_end='2024-11-30'),
    ], local_scheduler=True)
