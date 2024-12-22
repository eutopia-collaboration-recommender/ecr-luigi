import time

import luigi
import pandas as pd
import polars as pl
from tqdm import tqdm

from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case
from util.collaboration_novelty import (
    query_collaboration_novelty_batch,
    query_collaboration_novelty_num_batches,
    CollaborationNoveltyGraphTuple
)

from util.postgres import use_schema, write_table


class CalculateCollaborationNoveltyIndexTask(EutopiaTask):
    """
    Description: A Luigi task to embed the text of articles in the PostgreSQL database using a transformer model.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_size = self.config.COLLABORATION_NOVELTY.BATCH_SIZE
        self.min_year = self.config.COLLABORATION_NOVELTY.MIN_YEAR
        # Postgres settings
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_target_table_name_cni = 'collaboration_novelty_index'
        self.pg_target_table_name_cni_metadata = 'collaboration_novelty_metadata'
        # Delete before execution
        self.delete_insert = False

        # Initiate the Collaboration Novelty Graph Tuple
        self.GT = CollaborationNoveltyGraphTuple()

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
        N = query_collaboration_novelty_num_batches(conn=self.pg_connection,
                                                    batch_size=self.batch_size,
                                                    min_year=2000)
        self.logger.info(f"Found {N} batches to update")
        return list(range(N))

    def process_item(self, item: int) -> dict:
        self.logger.debug(f"Processing batch {item}")
        ix_batch = item
        # Query the collaborations for the batch
        start_time = time.time()
        df_collab = query_collaboration_novelty_batch(conn=self.pg_connection,
                                                      ix_batch=ix_batch,
                                                      batch_size=self.batch_size,
                                                      min_year=self.min_year)
        self.logger.debug(f"Queried batch {ix_batch} in {time.time() - start_time:.2f} seconds")
        # Get the unique articles in the batch
        articles = df_collab['article_id'].unique()
        # Loop through the articles in the batch
        start_time = time.time()
        CNI_rows, metadata_rows = list(), list()
        for article in tqdm(articles):
            # Filter the collaborations for the article
            df_article = df_collab.filter(pl.col('article_id') == article)
            # Update the Collaboration Novelty Graph Tuple with the article metadata and calculate the CNI
            CNI, metadata = self.GT.update(df_article)

            CNI_rows.append(CNI)
            metadata_rows.extend(metadata)

        self.logger.debug(f"Processed batch {ix_batch} in {time.time() - start_time:.2f} seconds")

        # Return the results
        return dict(
            cni=CNI_rows or [],
            metadata=metadata_rows or []
        )

    def checkpoint(self, iterable: list):
        use_schema(conn=self.pg_connection, schema=self.pg_target_schema)
        try:
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
        except KeyError:
            self.logger.error("No records to insert")
        except IndexError:
            self.logger.error("No records to insert")

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
