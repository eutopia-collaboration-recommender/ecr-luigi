import json
import time
import luigi

from dbt.cli.main import dbtRunner, dbtRunnerResult
from tasks.ingestion.parse_json import ParseJSONTask

from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class DbtDataIngestionTask(EutopiaTask):
    """
    Description: task running dbt to update the data ingestion mart including staging and marts models.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )
    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time())))

    def requires(self):
        return [
            ParseJSONTask(updated_date_start=self.updated_date_start, updated_date_end=self.updated_date_end)
        ]

    def run(self):
        self.logger.info(f"Running {self.__class__.__name__}.")
        # Execute dbt run (tag: data_ingestion)
        dbt_runner = dbtRunner()
        cli_args = ['run', '--project-dir', './dbt', '--select', 'tag:data_ingestion']
        res: dbtRunnerResult = dbt_runner.invoke(cli_args)

        # inspect the results
        for r in res.result:
            self.logger.info(f"{r.node.name}: {r.status}")
        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'task-finished': True})
            f.write(f"{result}")

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"dbt_data_ingestion_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([DbtDataIngestionTask()], local_scheduler=True)
