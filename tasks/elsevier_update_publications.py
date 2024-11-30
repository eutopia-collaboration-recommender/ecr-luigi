import json
import time
import luigi
import pandas as pd
from pymongo.synchronous.collection import Collection

from util.luigi.elsevier_task import ElsevierTask
from util.common import to_snake_case
from util.postgres import query, write_table
from datetime import datetime


def safe_get(record: dict, path: str, verbose: bool = False, default=None):
    """
    Safely get a value from a nested dictionary using a dot-separated path
    :param record: Record to extract the value from
    :param path: Dot-separated path to the value
    :param verbose: Print the path and record
    :return:  The value at the specified path or None if the path does not exist
    """
    # Split the path into keys
    for key in path.split('.'):
        if record is None or type(record) is not dict:
            # Print the path and record if the record is empty
            if verbose:
                print(f"Path: {path}, Record: {record}")
            # Return the default value if the record is empty
            if record is None:
                return default
            return record
        if key in record:
            record = record[key]

    return record


def parse_authors(record: dict):
    """
    Parse the authors from the record
    :param record: Record to extract the authors from
    :return: List of authors with their ID, name, and affiliation
    """
    # Check if the record is empty and it has author data
    if record is None:
        return []
    # In some cases the authors are stored in a separate field while by default they are stored in the 'dc:creator' field
    if 'authors' in record:
        authors = safe_get(record, 'authors.author', default=[])
    else:
        authors = safe_get(record, 'coredata.dc:creator', default=[])

    # Iterate over the authors and extract the author ID and name
    results = []
    for author in authors:
        # Check if the author has an affiliation
        if 'affiliation' not in author:
            affiliations = []
        # Affiliation can be a list or a dictionary - convert to a list
        elif type(author['affiliation']) is dict:
            affiliations = [author['affiliation']]
        else:
            affiliations = author['affiliation']

        # Process affiliations
        affiliation_ids = [{'id': affiliation['@id']} for affiliation in affiliations]
        try:
            results.append({
                'author_id': safe_get(author, '@auid'),
                'author_first_name': safe_get(author, 'ce:given-name'),
                'author_last_name': safe_get(author, 'ce:surname'),
                'author_indexed_name': safe_get(author, 'ce:indexed-name'),
                'author_initials': safe_get(author, 'ce:initials'),
                'author_affiliation_ids': affiliation_ids
            })
        except KeyError:
            print(f"Error for author: {author}")

    # Return the authors
    return results


def parse_keywords(record):
    """
    Parse the keywords from the record
    :param record: Record to extract the keywords from
    :return: List of keywords
    """
    keywords = safe_get(record, 'authkeywords.author-keyword')
    # Check if the record is empty and it has keyword data
    if record is None or keywords is None:
        return []

    # Extract the keywords from the record
    results = []
    for keyword in record['authkeywords']['author-keyword']:
        results.append({
            'keyword': safe_get(keyword, '$')
        })
    # Return the keywords
    return results


def parse_references(record):
    """
    Parse the references from the record
    :param record: Record to extract the references from
    :return: List of references with their ID and title
    """
    # Check if the record is empty and it has reference data
    references = safe_get(record, 'item.bibrecord.tail.bibliography.reference')
    if record is None or references is None:
        return []

    # In some cases there is only one reference and it is stored as a dictionary, convert to a list
    if type(references) is dict:
        references = [references]

    # Extract the references from the record
    results = []
    for reference in references:
        # Parse the reference title
        reference_title = safe_get(reference, 'ref-info.ref-title.ref-titletext')
        if reference_title is None:
            reference_title = safe_get(reference, 'ref-info.ref-text')
        # Append the reference to the list
        results.append({
            'reference_id': safe_get(reference, '@id'),
            'reference_title': reference_title
        })

    # Return the references
    return results


def parse_document(document):
    """
    Parse the document fields from the MongoDB document
    :param document: MongoDB document to extract the fields from
    :return: Dictionary with the publication fields
    """
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


def to_dataframe(publications: list):
    """
    Transform the modified records to a DataFrame
    :param publications: List of modified records
    :return: Pandas DataFrame with the modified records
    """
    # Create a DataFrame from the modified records
    df = pd.DataFrame(publications)
    df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

    # Transform field `publication_dt` and `publication_last_modification_dt` to date
    df['publication_dt'] = pd.to_datetime(df['publication_dt'])
    df['publication_last_modification_dt'] = pd.to_datetime(df['publication_last_modification_dt'])
    # Return the DataFrame
    return df


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

    def process_elsevier_publications(self, collection: Collection) -> list:
        """
        Process the Elsevier publications from the MongoDB collection
        :param collection: MongoDB collection to process
        :return: List of publications
        """

        # Create a filter for documents modified within the time range
        filter_query = {
            "last_modified": {
                "$gte": datetime.strptime(self.updated_date_start, "%Y-%m-%d"),
                "$lte": datetime.strptime(self.updated_date_end, "%Y-%m-%d")
            }
        }

        # Query all documents from each collection in batches
        cursor = collection.find(filter_query).batch_size(self.mongo_batch_size)  # Set batch size for the cursor

        # Setup variable to save the publications
        publications = []
        # Process each document in the cursor
        for ix, document in enumerate(cursor):
            # Extract the document fields
            publications.append(parse_document(document))
            if ix % self.mongo_batch_size == 0:
                self.logger.info(f"Processed {ix} records")

        # Return the publications
        return publications

    def run(self):
        """
        Run the main task. Process Elsevier publications. Write the results to the PostgreSQL database and save the
        number of rows written to a local target file.
        """

        self.logger.info(f"Running {self.__class__.__name__}.")

        try:
            # Limit to collection
            collection = self.mongo_db[self.mongo_collection_name]

            # Process the publications
            publications = self.process_elsevier_publications(collection)
        finally:
            # Close the connection to the MongoDB server
            self.mongo_client.close()

        # Convert the modified records to a DataFrame
        df = to_dataframe(publications=publications)

        # Write the DataFrame to the PostgreSQL database
        num_rows_written = write_table(conn=self.pg_connection, df=df, table_name=self.pg_target_table_name)

        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'num-rows-written': num_rows_written})
            f.write(f"{result}")

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
