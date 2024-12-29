import time
import re
import luigi
import pandas as pd

from time import sleep
from langchain_community.llms.ollama import Ollama

from tasks.data_ingestion import DataIngestionTask

from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case
from util.postgres import query


def prompt(article_description: str) -> str:
    return f"""
    You are an expert in extracting meaningful keywords from academic content. 
    When selecting keywords, prioritize terms or phrases that are widely recognized, commonly used, and broadly relevant within the academic domain, rather than highly specific or unique terms. 
    Limit the selection to the top 5 most relevant and general keywords or key phrases that summarize the main topics, themes, or concepts. 
    Format the result as a comma-separated list. All keywords should be in English, lowercase and abbreviations should be spelled out.
    You are not allowed to include any kind of explanations, notes or any additional content in the output besides the keywords themselves.  

    Input Text: {article_description}

    Output: [Keyword1, Keyword2, Keyword3, Keyword4, Keyword5]
    """


class GenerateArticleKeywordsTask(EutopiaTask):
    """
    Description: A Luigi task to generate article keywords using the Ollama model
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'article_keywords'
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.num_records_to_checkpoint = self.config.BATCH_SIZE.SMALL
        # Load the Ollama model
        self.ollama = Ollama(
            base_url=self.config.OLLAMA.HOST,
            headers={"Authorization": f"Bearer {self.config.OLLAMA.API_KEY}"},
            model=self.config.OLLAMA.MODEL
        )

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
                   'Title: ' || s.article_title                 AS article_title,
                   'Journal title: ' || s.article_journal_title AS article_journal_title,
                   'Abstract: ' || s.article_abstract           AS article_abstract,
                   'References: ' || s.article_references       AS article_references
            FROM stg_mart_article s
            LEFT JOIN article_keywords t 
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
        document = item
        document_inputs = ', '.join([
            document['article_title'] or '',
            document['article_journal_title'] or '',
            document['article_abstract'] or '',
            document['article_references'] or ''
        ])
        try:
            # Prompt the user for keywords
            keywords = self.ollama.invoke(
                prompt(article_description=document_inputs)
            )
            # Clean the keywords
            keywords = keywords.strip().lstrip('[').rstrip(']')
            keywords = keywords.replace('{', '')
            keywords = keywords.replace('}', '')
            keywords = list(
                filter(lambda x: x != '',
                       map(
                           str.strip, keywords.split(','))
                       )
            )
            keywords = ', '.join(keywords)
            keywords = '{' + keywords + '}'
            if re.search(r'[\r\n]', keywords):
                # Log the issue
                self.logger.error(f"Keywords contain newline characters: {keywords}")
                # Reset the keywords
                keywords = '{}'

        # If we get a ValueError, sleep for 5 seconds and try again
        except ValueError as e:
            keywords = '{}'
            self.logger.error(f"Error generating keywords: {e}")
            sleep(30)

        return dict(
            article_id=document['article_id'],
            article_keywords_arr=keywords
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
        target_name = f"generate_article_keywords_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        GenerateArticleKeywordsTask(),
    ], local_scheduler=True)
