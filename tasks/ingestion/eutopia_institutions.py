import luigi

from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.luigi.eutopia_task import EutopiaTask


class EutopiaInstitutionsTask(EutopiaTask):
    """
    Description: A Luigi task transforming the EUTOPIA_INSTITUTION_REGISTRY into a table of EUTOPIA institutions and
    saving the table to Postgres.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'eutopia_institution'

    def query_records_to_update(self) -> list:
        """
        Get EUTOPIA institutions from the EUTOPIA_INSTITUTION_REGISTRY
        :return: List of EUTOPIA institutions
        """
        institutions = [EUTOPIA_INSTITUTION_REGISTRY[university] for university in EUTOPIA_INSTITUTION_REGISTRY.keys()]
        return institutions

    def process_item(self, item: str) -> dict:
        """
        Process a single item
        :param item: EUTOPIA institution metadata
        :return: Same item
        """
        return item

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        target_name = f"eutopia_institutions"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        EutopiaInstitutionsTask(),
    ], local_scheduler=True)
