import json
import time
import luigi
import pandas as pd

from util.elsevier.parse import (
    parse_authors,
    parse_keywords,
    parse_references,
    safe_get
)
from util.luigi.elsevier_task import ElsevierTask
from util.common import to_snake_case
from datetime import datetime


class ElsevierUpdatePublicationsTask(ElsevierTask):
    """
    Description: Task to update the Elsevier publications in the PostgreSQL database. The task fetches the modified
    Elsevier publications from MongoDB, processes the publications, and writes the results to the PostgreSQL database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'elsevier_publication'
        self.mongo_collection_name = 'pub_meta'

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default='2024-05-01'
        # default = time.strftime("%Y-%m-%d",
        #                     time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )

    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default='2024-05-31'
        # default=time.strftime("%Y-%m-%d", time.gmtime(time.time()))
    )

    def query_records_to_update(self) -> list:
        """
        Process the Elsevier publications from the MongoDB collection
        :param collection: MongoDB collection to process
        :return: List of publications
        """

        # Limit to collection
        collection = self.mongo_db[self.mongo_collection_name]

        # Create a filter for documents modified within the time range
        filter_query = {
            "last_modified": {
                "$gte": datetime.strptime(self.updated_date_start, "%Y-%m-%d"),
                "$lte": datetime.strptime(self.updated_date_end, "%Y-%m-%d")
            }
        }

        # Query all documents from each collection in batches
        cursor = collection.find(filter_query).batch_size(self.mongo_batch_size)  # Set batch size for the cursor
        return cursor

    def process_item(self, item: dict) -> dict:
        """
        Parse the document fields from the MongoDB document
        :param item: MongoDB document to extract the fields from
        :return: Dictionary with the publication fields
        """
        document = item
        # Extract the publication fields
        record = document.get('record', None)
        result = {
            # Publication metadata
            'publication_id': document.get('scopus_id', None),
            'publication_eid': safe_get(record, 'coredata.eid'),
            'publication_doi': safe_get(record, 'coredata.prism:doi'),
            'publication_title': safe_get(record, 'coredata.dc:title'),
            'publication_type': safe_get(record, 'coredata.prism:aggregationType'),
            'publication_abstract': safe_get(record, 'coredata.dc:description'),
            'publication_citation_count': safe_get(record, 'coredata.citedby-count'),
            'publication_dt': safe_get(record, 'coredata.prism:coverDate'),
            'publication_last_modification_dt': document.get('last_modified', None),
            # Authors
            'publication_authors': json.dumps(parse_authors(record)),
            # Keywords
            'publication_keywords': json.dumps(parse_keywords(record)),
            # References
            'publication_references': json.dumps(parse_references(record))
        }

        # Return the publication
        return result

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Transform the modified records to a DataFrame
        :param publications: List of modified records
        :return: Pandas DataFrame with the modified records
        """
        # Create a DataFrame from the modified records
        df = pd.DataFrame(iterable)
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

        # Transform field `publication_dt` and `publication_last_modification_dt` to date
        df['publication_dt'] = pd.to_datetime(df['publication_dt'])
        df['publication_last_modification_dt'] = pd.to_datetime(df['publication_last_modification_dt'])
        # Return the DataFrame
        return df

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"elsevier_publications_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        ElsevierUpdatePublicationsTask(),
    ], local_scheduler=True)
