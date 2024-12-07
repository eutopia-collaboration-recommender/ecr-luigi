import luigi

from bs4 import BeautifulSoup

from util.luigi.eutopia_task import EutopiaTask


class CerifResearchAreasTask(EutopiaTask):
    """
    Description: A Luigi task to fetch the research topics from the CERIF registry (https://www.arrs.si/sl/gradivo/sifranti/sif-cerif-cercs.asp).
    For simplicity purposes the HTML page is downloaded manually and the research topics are extracted from the HTML within the task.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'cerif_research_area'
        self.html_file_path = 'data/cerif.html'

    def query_records_to_update(self) -> list:
        """
        Query the DOIs from the PostgreSQL database
        :return: List of DOIs
        """

        # Query all research topics from the CERIF registry
        with open(self.html_file_path, 'r', encoding='windows-1250') as file:
            html_content = file.read()

        # Parse the HTML content
        html_parsed = BeautifulSoup(html_content, 'html.parser')

        return html_parsed.find_all('tr')

    def process_item(self, item: str) -> dict:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        return dict()

    def process(self, iterable: list):
        # Initialize variables to keep track of headers
        current_research_branch = dict(research_branch_code='n/a', research_branch_name='Other')
        current_research_subbranch = dict(research_branch_code='n/a', research_branch_name='Other')

        # Initialize lists to store the extracted data
        research_areas = list()

        for row in iterable:
            # Check for research_branch_level defined as <tr> having class='BarvaGlava', colspan=2 and a <b> tag inside with
            # branch name, a space, a letter, another space and finally 3 digits (the letter and 3 digits are the branch
            # code)
            row_clazz = row.get('class')
            if row_clazz is None:
                continue
            if 'BarvaGlava' in row_clazz and row.find('td', {'colspan': '2'}) and row.find('b'):
                current_research_branch = dict(
                    research_branch_code=' '.join(row.find('b').get_text(strip=True).split(' ')[-2:]),
                    research_branch_name=' '.join(row.find('b').get_text(strip=True).split(' ')[:-2])
                )
                # Reset the subbranch
                current_research_subbranch = dict(research_branch_code='n/a', research_branch_name='Other')

            # Check for research_subbranch_level defined as having class='BarvaTR2', colspan=2 and a <b> tag inside with
            # subbranch name, a space, a letter, another space and finally 3 digits (the letter and 3 digits are the
            # subbranch code)
            elif 'BarvaTR2' in row_clazz and row.find('td', {'colspan': '2'}) and row.find('b'):
                current_research_subbranch = dict(
                    research_branch_code=' '.join(row.find('b').get_text(strip=True).split(' ')[-2:]),
                    research_branch_name=' '.join(row.find('b').get_text(strip=True).split(' ')[:-2])
                )
            # Check for research_area_level defined as having class='BarvaTR1'. 2 <td> tags inside with the area code
            # and the area name
            elif 'BarvaTR1' in row_clazz:
                # Get the area code and name
                area_code, area_name = row.find_all('td')

                # Append the extracted data to the list
                research_areas.append(dict(
                    research_branch_code=current_research_branch['research_branch_code'],
                    research_branch_name=current_research_branch['research_branch_name'],
                    research_subbranch_code=current_research_subbranch['research_branch_code'],
                    research_subbranch_name=current_research_subbranch['research_branch_name'],
                    research_area_code=area_code.get_text(strip=True),
                    research_area_name=area_name.get_text(strip=True),
                ))
        return research_areas

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        target_name = f"cerif_research_areas"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        CerifResearchAreasTask(),
    ], local_scheduler=True)
