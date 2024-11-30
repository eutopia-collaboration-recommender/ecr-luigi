import json
import logging
import time

import luigi
import pandas as pd

from box import Box
from util.postgres import create_connection, write_table


class EutopiaTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Read settings from config file
        # Note: The config file should be stored in the same directory where script is executed
        self.config = Box.from_yaml(filename="config.yaml")
        self.logger = logging.getLogger()

        self.pg_target_table_name = None

        # PostgreSQL connection
        self.pg_connection = create_connection(
            username=self.config.POSTGRES.USERNAME,
            password=self.config.POSTGRES.PASSWORD,
            host=self.config.POSTGRES.HOST,
            port=self.config.POSTGRES.PORT,
            database=self.config.POSTGRES.DATABASE,
            schema=self.config.POSTGRES.SCHEMA
        )

    def to_dataframe(self, iterable: list):
        """
        Transform the modified records to a DataFrame
        :param iterable: List of modified records
        :return: Pandas DataFrame with the modified records
        """
        # Create a DataFrame from the modified records
        df = pd.DataFrame(iterable)
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

        # Return the DataFrame
        return df

    def on_run_finished(self, iterable: list):
        # Convert the modified records to a DataFrame
        df = self.to_dataframe(iterable=iterable)

        # Write the DataFrame to the PostgreSQL database
        num_rows_written = write_table(conn=self.pg_connection, df=df, table_name=self.pg_target_table_name)

        # Close the PostgreSQL connection
        self.pg_connection.close()
        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'num-rows-written': num_rows_written})
            f.write(f"{result}")

        self.logger.info(f"Number of rows written to `{self.pg_target_table_name}`: {num_rows_written}")
