import time
import luigi
import pandas as pd

from langdetect import detect, LangDetectException

from tasks.data_ingestion import DataIngestionTask

from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case
from util.postgres import query


def classify_article_language(article_description: str) -> str:
    """
    Processes a single article by detecting the language of the article.
    :param item: The item to process.
    :param metadata: The metadata for the current iteration.
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far.
    :return: The updated settings for the current iteration.
    """

    try:
        return detect(text=article_description)
    except LangDetectException:
        return 'n/a'


class ClassifyArticleLanguageTask(EutopiaTask):
    """
    Description: A Luigi task to classify the language of articles in the PostgreSQL database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'article_language'
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA

        # Delete before execution
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
        return [
            DataIngestionTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end)
        ]

    def query_records_to_update(self) -> list:
        """
        Query the articles and corresponding texts that will be embedded from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
            SELECT s.article_id,
                   s.article_title         AS article_title,
                   s.article_journal_title AS article_journal_title,
                   s.article_abstract      AS article_abstract,
                   s.article_references    AS article_references
            FROM stg_mart_article s
                     LEFT JOIN article_language t
                               ON S.article_id = t.article_id
            WHERE t.article_id IS NULL
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)

        # To list of dictionaries
        articles = df.to_dict(orient='records')

        # Log the number of articles to process
        self.logger.info(f"Number of articles to process (generating keywords): {len(articles)}")

        # Return the articles
        return articles

    def process_item(self, item: list) -> dict:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """

        # Get the articles
        article = item
        article_description = ', '.join([
            article['article_title'] or '',
            article['article_journal_title'] or '',
            article['article_abstract'] or '',
            article['article_references'] or ''
        ])

        # Classify the language
        try:
            language = detect(text=article_description)
        except LangDetectException:
            language = 'n/a'

        # Log the time taken
        return dict(
            article_id=article['article_id'],
            article_language=language
        )

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Convert the iterable to a DataFrame
        :param iterable: List of dictionaries
        :return: DataFrame
        """
        return pd.DataFrame(iterable)

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"article_language_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        ClassifyArticleLanguageTask(),
    ], local_scheduler=True)
