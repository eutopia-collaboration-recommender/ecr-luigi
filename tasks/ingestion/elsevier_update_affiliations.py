import json
import time
import luigi

from datetime import datetime

from util.common import to_snake_case
from util.luigi.elsevier_task import ElsevierTask
from util.elsevier.parse import (
    parse_affiliations,
    safe_get
)


class ElsevierUpdateAffiliationsTask(ElsevierTask):
    """
    Description: Task to update the Elsevier affiliations in the PostgreSQL database. The task fetches all the
    Elsevier affiliations from MongoDB, processes them and writes the results to the PostgreSQL database. It filters
    the affiliations based on the last modified date.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'elsevier_publication_affiliation'
        self.mongo_collection_name = 'pub_aff'

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )

    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time()))
    )

    def query_records_to_update(self) -> list:
        """
        Process the Elsevier affiliations from the MongoDB collection
        :param collection: MongoDB collection to process
        :return: List of affiliations
        """

        # Limit to collection
        collection = self.mongo_db[self.mongo_collection_name]

        filter_query = {
            "last_modified": {
                "$gte": datetime.strptime(self.updated_date_start, "%Y-%m-%d"),
                "$lte": datetime.strptime(self.updated_date_end, "%Y-%m-%d")
            }
        }
        # Query all documents from each collection in batches
        cursor = collection.find(filter_query).batch_size(self.mongo_batch_size)  # Set batch size for the cursor
        return cursor

    def process_item(self, item: dict) -> list:
        """
        Parse the document fields from the MongoDB document
        :param item: MongoDB document to extract the fields from
        :return: Dictionary with the publication fields
        """
        document = item

        affiliation_id = document.get('af_id')

        # Initialize the affiliatied_publications
        affiliated_publications = []
        # Iterate over the records and extract the affiliation metadata
        for record_id in document.get('records', []):
            record = safe_get(document, f'records.{record_id}')
            # Extract the publication metadata
            affiliated_publications.append({
                'publication_id': safe_get(record, 'dc:identifier'),
                'publication_eid': safe_get(record, 'eid'),
                'publication_doi': safe_get(record, 'prism:doi'),
                # Affiliation metadata
                'publication_affiliation_id': affiliation_id,
                'publication_affiliations': json.dumps(parse_affiliations(record=record))
            })

        # Return the affiliated publications
        return affiliated_publications

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"elsevier_affiliations_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        ElsevierUpdateAffiliationsTask(),
    ], local_scheduler=True)
