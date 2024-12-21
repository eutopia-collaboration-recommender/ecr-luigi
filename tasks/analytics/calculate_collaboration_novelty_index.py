import time

import luigi
import pandas as pd
import networkx as nx
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case

from util.collaboration_novelty.process import process_article_collaboration_novelty
from util.collaboration_novelty.query import query_collaboration_batch, query_collaboration_n_batches
from util.postgres import use_schema, write_table


class CalculateCollaborationNoveltyIndexTask(EutopiaTask):
    """
    Description: A Luigi task to embed the text of articles in the PostgreSQL database using a transformer model.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_size = self.config.COLLABORATION_NOVELTY.BATCH_SIZE
        # Postgres settings
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_target_table_name_cni = 'collaboration_novelty_index'
        self.pg_target_table_name_cni_metadata = 'collaboration_novelty_metadata'
        # Delete before execution
        self.delete_insert = False

        # Initiate graphs
        self.G_a = nx.Graph()
        self.G_i = nx.Graph()

        # Checkpoint on each iteration
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

    def query_records_to_update(self) -> list:
        N = query_collaboration_n_batches(conn=self.pg_connection,
                                          batch_size=self.batch_size,
                                          min_year=2000)
        self.logger.info(f"Found {N} batches to update")
        return list(range(N))

    def process_item(self, item: list) -> dict:
        df_batch = query_collaboration_batch(conn=self.pg_connection,
                                             batch_size=self.batch_size,
                                             min_year=2000)
        self.logger.info(f"Processing batch index {item} with {len(df_batch)} records")

        # Initialize the lists to store the collaboration novelty index and the metadata
        cni_rows, metadata_rows = list(), list()

        # Iterate through all the articles
        for iy, article_id in enumerate(df_batch['article_id'].unique()):
            # Get the article rows
            df_batch_article = df_batch[df_batch['article_id'] == article_id]

            # Process the article
            cni_row_i, metadata_rows_i = process_article_collaboration_novelty(
                article_id=article_id,
                df=df_batch_article,
                G_a=self.G_a,
                G_i=self.G_i,
            )

            # Append the collaboration to the lists
            cni_rows.append(cni_row_i)
            metadata_rows.extend(metadata_rows_i)

        return dict(
            cni=cni_rows,
            metadata=metadata_rows
        )

    def checkpoint(self, iterable: list):
        use_schema(conn=self.pg_connection, schema=self.pg_target_schema)

        cni_rows, metadata_rows = iterable[0]['cni'], iterable[0]['metadata']
        df_cni = self.to_dataframe(cni_rows)
        df_metadata = self.to_dataframe(metadata_rows)

        # Write the results to collaboration_novelty_index table
        write_table(conn=self.pg_connection,
                    table_name=self.pg_target_table_name_cni,
                    df=df_cni)

        # Write the results to collaboration_novelty_index_metadata table
        write_table(conn=self.pg_connection,
                    table_name=self.pg_target_table_name_cni_metadata,
                    df=df_metadata)

        self.logger.info(
            f"Inserted {len(df_metadata)} and {len(df_cni)} records into collaboration_novelty_index_metadata and collaboration_novelty_index tables, respectively")

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
        target_name = f"calculate_collaboration_novelty_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        CalculateCollaborationNoveltyIndexTask(),
    ], local_scheduler=True)
