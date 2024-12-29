import time
import luigi
import json

from tasks.ingestion.crossref_top_n_research_area_publications import CrossrefTopNResearchAreaPublicationsTask
from tasks.ingestion.crossref_update_publications import CrossrefUpdatePublicationsTask
from tasks.ingestion.elsevier_update_affiliations import ElsevierUpdateAffiliationsTask
from tasks.ingestion.elsevier_update_publications import ElsevierUpdatePublicationsTask
from tasks.ingestion.eutopia_institutions import EutopiaInstitutionsTask
from tasks.ingestion.orcid_update_member_employments import OrcidUpdateMemberEmploymentsTask
from tasks.ingestion.orcid_update_member_person import OrcidUpdateMemberPersonTask

from util.eutopia import EUTOPIA_INSTITUTION_REGISTRY
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case


class ParseJSONTask(EutopiaTask):
    """
    Description: parse JSON data from Elsevier, Crossref, and ORCID for the given time period using stored
    procedures in PostgreSQL.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_table_name = 'orcid_modified_member'
        self.pg_target_table_name = 'orcid_member_works'

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )
    updated_date_end: str = luigi.OptionalParameter(description='Search end date',
                                                    default=time.strftime("%Y-%m-%d", time.gmtime(time.time())))

    def requires(self):
        """
        Requires all tasks to be completed before this task can be run.
        :return: List of tasks
        """
        tasks = list()
        # Get EUTOPIA institutions
        tasks.append(EutopiaInstitutionsTask())
        # Get CERIF research areas and top N publications for each research area
        tasks.append(CrossrefTopNResearchAreaPublicationsTask())
        # Update Elsevier publications and affiliations
        tasks.append(ElsevierUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                    updated_date_end=self.updated_date_end))
        tasks.append(ElsevierUpdateAffiliationsTask(updated_date_start=self.updated_date_start,
                                                    updated_date_end=self.updated_date_end))

        # Update Crossref publications
        # ** Note that this also triggers fetching ORCID modified members and ORCID member works for all EUTOPIA institutions
        tasks.append(CrossrefUpdatePublicationsTask(updated_date_start=self.updated_date_start,
                                                    updated_date_end=self.updated_date_end))

        # Update ORCID member metadata for all EUTOPIA institutions
        tasks.extend(
            [OrcidUpdateMemberPersonTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['institution_pretty_name'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end)
                for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()]
        )
        tasks.extend(
            [OrcidUpdateMemberEmploymentsTask(
                affiliation_name=EUTOPIA_INSTITUTION_REGISTRY[institution_id]['institution_pretty_name'],
                updated_date_start=self.updated_date_start,
                updated_date_end=self.updated_date_end)
                for institution_id in EUTOPIA_INSTITUTION_REGISTRY.keys()]
        )

        # Return requirements
        return tasks

    def run(self):
        """
        Run the main task. Update ORCID works JSON for each modified ORCID record in given timeframe.
        Write the results to the PostgreSQL database and save the number of rows written to a local target file.
        """
        self.logger.info(f"Running {self.__class__.__name__}.")

        min_year = self.updated_date_start.split('-')[0]
        max_year = self.updated_date_end.split('-')[0]

        # Execute parsing JSON stored procedures in Postgres
        for i in range(int(min_year), int(max_year) + 1):
            self.logger.info(f"Running {self.__class__.__name__} for year {i}.")

            sql_query_crossref = f"""
            CALL sp_parse_crossref_publications(CONCAT({i}, '-01-01')::DATE, CONCAT({i}, '-12-31')::DATE);
            """

            sql_query_elsevier = f"""
            CALL sp_parse_elsevier_publications(CONCAT({i}, '-01-01')::DATE, CONCAT({i}, '-12-31')::DATE);
            """

            sql_query_orcid = f"""
            CALL sp_parse_orcid_member_works({i});
            """

            with self.pg_connection.cursor() as cursor:
                cursor.execute(sql_query_crossref)
                self.logger.info(f"Finished running sp_parse_crossref_publications for year {i}.")
                cursor.execute(sql_query_elsevier)
                self.logger.info(f"Finished running sp_parse_elsevier_publications for year {i}.")
                cursor.execute(sql_query_orcid)
                self.logger.info(f"Finished running sp_parse_orcid_member_works for year {i}.")

                self.pg_connection.commit()
            cursor.close()

        # Close the connections
        self.close_connection()

        # Save number of rows written local target
        with self.output().open('w') as f:
            result = json.dumps({'num-rows-written': self.num_rows_written})
            f.write(f"{result}")

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"parse_json_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        ParseJSONTask()
    ], local_scheduler=True)
