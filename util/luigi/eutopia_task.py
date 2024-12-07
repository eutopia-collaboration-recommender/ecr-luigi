import json
import logging
import time
from typing import Iterable

import luigi
import pandas as pd

from box import Box
from util.postgres import create_connection, use_schema, write_table


class EutopiaTask(luigi.Task):
    """
    This is a base class for all Eutopia tasks. It provides the common functionality for all tasks.
    The structure of the task is as follows:
    1. Checks if the local target file exists and returns the path to the file using the `output` method.
    1. Initializes the task with config.yaml settings and PostgreSQL connection.
    2. Check if there are any requirements for the task (Luigi default)
    3. We start executing the `run` method
        3.1. Fetch records to process using the `query_records_to_update` method.
        3.2. Process the records using the `process` method. This method also logs the progress, checkpoints the results
                to the PostgreSQL database and breaks the loop if in development environment.
        !!        - The `process_item` method should be implemented in the child class: it processes a single item.
        !!        - The `to_dataframe` method should be implemented in the child class: it transforms the modified records.
        3.3. Write the remaining results to the PostgreSQL database.
        3.4. Close the connections.
        3.5. Save the number of rows written to the local target file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Read settings from config file
        # Note: The config file should be stored in the same directory where script is executed
        self.config = Box.from_yaml(filename="config.yaml")
        self.logger = logging.getLogger()

        # Read environment
        self.environment = self.config.ENVIRONMENT

        # Set Postgres target table variable to None by default
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

        self.pg_source_schema = self.config.POSTGRES.SCHEMA
        self.pg_target_schema = self.config.POSTGRES.SCHEMA

        # Variable to save the number of written rows
        self.num_rows_written = 0
        # Specifications for logging, checkpointing and breaking in development
        self.num_records_to_log = 50
        self.num_records_to_checkpoint = 100
        self.num_records_to_break_in_dev = 500
        # Specification for delete insert procedure in the target table (if needed)
        self.delete_insert = True
        self.params_spec = "n/a"

    def checkpoint(self, iterable: list):
        # Check if there are any records to write
        if len(iterable) == 0:
            self.logger.info(f"No rows to write to `{self.pg_target_table_name}`.")
            return

        # Convert the modified records to a DataFrame
        df = self.to_dataframe(iterable=iterable)

        # Write the DataFrame to the PostgreSQL database
        num_rows_written = write_table(conn=self.pg_connection, df=df, table_name=self.pg_target_table_name)

        self.logger.info(f"Number of rows written to `{self.pg_target_table_name}`: {num_rows_written}")

        # Add to number of written rows
        self.num_rows_written += num_rows_written

    def process_item(self, item):
        """
        Process the item using the provided function.
        :param item: Item to process
        :return: Processed item
        """
        raise NotImplementedError

    def process(self, iterable: Iterable) -> list:
        """
        Process the iterable using the provided function.
        :param iterable: Iterable to process
        :return: List of processed items
        """
        # Initialize the results list
        results = list()

        # Iterate over the iterable and process each item
        for ix, item in enumerate(iterable):

            # Process item
            result = self.process_item(item=item)

            # Check if the result is a list and extend the results list
            if type(result) is list:
                results.extend(result)
            # Append the processed item to the results list if it is not a list
            else:
                results.append(self.process_item(item=item))

            # Skip first iteration
            if ix == 0:
                continue
            else:
                # Print progress
                if ix % self.num_records_to_log == 0:
                    self.logger.info(f"Processed {ix} records")
                # Break the loop if in development environment and the number of records to break in development is reached
                if ix == self.num_records_to_break_in_dev and self.environment == 'dev':
                    break
                # Checkpoint the results to Postgres
                if ix % self.num_records_to_checkpoint == 0:
                    self.checkpoint(iterable=results)
                    results = list()

        # Return the results
        return results

    def delete_processed_records(self) -> None:
        """
        Delete the processed records from the target table
        """
        # Use target schema
        use_schema(conn=self.pg_connection, schema=self.pg_target_schema)
        # Create a string representation of the task parameters
        task_params_spec = ','.join([f'{key}:{str(value)}' for key, value in self.param_kwargs.items()])
        self.params_spec = task_params_spec if task_params_spec != '' else self.params_spec
        # Delete the processed records from the target table if the delete_insert flag is set to True
        if self.delete_insert:
            query_str = f"DELETE FROM {self.pg_target_table_name} WHERE task_params_spec = '{self.params_spec}'"
            with self.pg_connection.cursor() as cursor:
                cursor.execute(query_str)
                self.pg_connection.commit()

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Transform the modified records to a DataFrame
        :param iterable: List of modified records
        :return: Pandas DataFrame with the modified records
        """
        # Create a DataFrame from the modified records
        df = pd.DataFrame(iterable)
        df['row_created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
        df['task_params_spec'] = self.params_spec

        # Return the DataFrame
        return df

    def query_records_to_update(self) -> list:
        """
        Query the records to update from the PostgreSQL database
        :return: List of records to update
        """
        raise NotImplementedError

    def run(self):
        """
        Run the main task. Process Elsevier publications. Write the results to the PostgreSQL database and save the
        number of rows written to a local target file.
        """

        self.logger.info(f"Running {self.__class__.__name__}.")

        # Delete processed records (if needed)
        self.delete_processed_records()

        # Use source schema
        use_schema(conn=self.pg_connection, schema=self.pg_source_schema)
        # Fetch records to process
        iterable = self.query_records_to_update()

        # Use target schema
        use_schema(conn=self.pg_connection, schema=self.pg_target_schema)
        # Process the records
        iterable_processed = self.process(iterable=iterable)

        # Write the DataFrame to the PostgreSQL database
        self.on_run_finished(iterable=iterable_processed)

    def on_run_finished(self, iterable: list):
        # Save data to the target table
        self.checkpoint(iterable=iterable)

        # Close the connections
        self.close_connection()

        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'num-rows-written': self.num_rows_written})
            f.write(f"{result}")

    def close_connection(self):
        self.pg_connection.close()
