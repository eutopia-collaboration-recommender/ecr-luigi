import luigi

from tasks.ingestion.cerif_research_areas import CerifResearchAreasTask

from util.crossref.works import query_top_n_by_keyword
from util.luigi.crossref_task import CrossrefTask
from util.postgres import query


class CrossrefTopNResearchAreaPublicationsTask(CrossrefTask):
    """
    Description: A Luigi task to fetch the publication metadata from Crossref for the DOIs in the PostgreSQL database
    from ORCID member works. By default, the task fetches the publication metadata for the DOIs that have been updated
    in the last 7 days. The task requires the OrcidUpdateMemberWorksTask to be completed before it can be run.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'crossref_top_n_research_area_publication'
        self.num_top_research_area_publications = self.config.CROSSREF.NUM_TOP_RESEARCH_AREA_PUBLICATIONS

    def requires(self):
        """
        The task requires the CerifResearchAreasTask to be completed before it can be run.
        :return: List of all required tasks
        """
        return [CerifResearchAreasTask()]

    def query_records_to_update(self) -> list:
        """
        Query the DOIs from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
            SELECT research_area_name,
                     research_area_code
            FROM cerif_research_area
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)
        return df.to_dict(orient='records')

    def process_item(self, item: str) -> list:
        """
        Process a single item
        :param item: Research area dictionary with research area name and code
        :return: Record with publication metadata
        """
        research_area = item

        # Query the top N DOIs by keyword concatenated to a string to be input into the text embedding model
        top_n_publications = query_top_n_by_keyword(base_url=self.base_url,
                                                    params=self.params,
                                                    keyword=research_area['research_area_name'],
                                                    n=self.num_top_research_area_publications)

        # Add research area code to the list
        top_n_publications = [
            {
                "publication_doi": publication['publication_doi'],
                "publication_metadata": publication['publication_metadata'],
                "cerif_research_area_code": research_area['research_area_code']
            } for publication in top_n_publications
        ]

        # Return the processed publications
        return top_n_publications

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        target_name = f"crossref_top_n_research_area_publications"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        CrossrefTopNResearchAreaPublicationsTask(),
    ], local_scheduler=True)
