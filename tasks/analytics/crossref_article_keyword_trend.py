import time
import luigi
import pandas as pd

from util.crossref.works import query_keyword_trend
from util.luigi.crossref_task import CrossrefTask
from util.common import to_snake_case
from util.postgres import query


class CrossrefArticleKeywordTrend(CrossrefTask):
    """
    Description:
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'article_keyword_trend'
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.min_year = self.config.MIN_YEAR
        self.max_year = time.gmtime(time.time()).tm_year
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

    def query_records_to_update(self) -> list:
        """
        Query the DOIs from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
            WITH keywords_to_load AS (SELECT DISTINCT keyword AS article_keyword
                                      FROM article_keywords,
                                           LATERAL UNNEST(article_keywords_arr) AS keyword)
            SELECT a.article_keyword
            FROM keywords_to_load a
                     LEFT JOIN article_keyword_trend k
                               ON a.article_keyword = k.article_keyword
            WHERE k.article_keyword IS NULL
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)
        return df['article_keyword'].tolist()

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Convert the iterable to a DataFrame
        :param iterable: List of records
        :return: DataFrame
        """
        return pd.DataFrame(iterable)

    def process_item(self, item: str) -> list:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        keyword = item
        # Fetch the keyword trend
        records = query_keyword_trend(
            base_url=self.base_url,
            params=self.params,
            keyword=keyword,
            start_year=self.min_year,
            end_year=self.max_year
        )

        # Return the records
        return records

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"crossref_article_keywords_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        CrossrefArticleKeywordTrend(updated_date_start='2024-01-01', updated_date_end='2024-11-30'),
    ], local_scheduler=True)
