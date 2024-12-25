import time
import luigi
import numpy as np
import pandas as pd
from tqdm import tqdm

from util.embedding import cosine_similarity
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case
from util.postgres import query


class ClassifyArticleResearchAreaTask(EutopiaTask):
    """
    Description: A Luigi task to generate article keywords using the Ollama model
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'article_research_area'
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.batch_size = self.config.BATCH_SIZE.LARGE

        # Delete before execution
        self.delete_insert = False

        self.df_top_n_research_area_embeddings = None
        self.top_n_research_area_embedding_values = None
        self.num_records_to_checkpoint = 1

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )

    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time()))
    )

    def query_top_n_research_area_embeddings(self) -> pd.DataFrame:
        """
        Query the top n research area embeddings
        :return: DataFrame with top n research area embeddings
        """
        query_str = f"""
            SELECT distinct t.article_doi,
                             t.article_text_embedding,
                             r.research_area_code
             FROM top_n_research_area_article_text_embedding t
                      LEFT JOIN stg_crossref_top_n_research_area_article r
                                ON t.article_doi = r.article_doi
        """
        # Fetch the DOIs from the PostgreSQL database
        return query(conn=self.pg_connection,
                     query=query_str)

    def query_records_to_update(self) -> list:
        """
        Query the articles and corresponding texts that will be embedded from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
            SELECT CEIL(COUNT(DISTINCT s.article_id) / {self.batch_size}::FLOAT) AS N
            FROM article_text_embedding s
                     LEFT JOIN article_research_area t
                               ON S.article_id = t.article_id
            WHERE t.article_id IS NULL
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)

        # Inititalize the top N research area embeddings
        self.df_top_n_research_area_embeddings = self.query_top_n_research_area_embeddings()
        self.top_n_research_area_embedding_values = np.vstack(
            self.df_top_n_research_area_embeddings['article_text_embedding'])

        # Return the number of batches
        return list(range(df['n'].values[0]))

    def classify_article_research_area(self, article_text_embedding: np.ndarray) -> list:
        """
        Classify the article research area
        :param article_text_embedding: Article text embedding
        :return: Research area
        """

        # Calculate the cosine similarity between the research topics and the article
        similarities = cosine_similarity(vector=article_text_embedding,
                                         matrix=self.top_n_research_area_embedding_values)

        # Find the 3 top most similar research topics
        top_3_topics = np.argsort(similarities)[::-1][:3]

        return [
            dict(
                research_area_code=self.df_top_n_research_area_embeddings['research_area_code'].iloc[topic],
                research_area_rank=rank
            )
            for rank, topic in enumerate(top_3_topics, 1)
        ]

    def process_item(self, item: list) -> list:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        query_str = f"""
            SELECT s.article_id,
                   s.article_text_embedding
            FROM article_text_embedding s
                     LEFT JOIN article_research_area t
                               ON S.article_id = t.article_id
            WHERE t.article_id IS NULL
            LIMIT {self.batch_size}
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)

        # Process the articles
        results = list()
        for ix, row in tqdm(df.iterrows()):
            article_id = row['article_id']
            article_text_embedding = np.array(row['article_text_embedding'])

            # Classify the article language
            article_research_areas = self.classify_article_research_area(article_text_embedding=article_text_embedding)
            article_research_areas = [
                dict(
                    article_id=article_id,
                    **research_area
                )
                for research_area in article_research_areas
            ]

            # Append the article to the list
            results.extend(article_research_areas)

        # Return the articles
        return results

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
        target_name = f"article_research_area_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        ClassifyArticleResearchAreaTask(),
    ], local_scheduler=True)
