def safe_get(record: dict, path: str, verbose: bool = False, default=None):
    """
    Safely get a value from a nested dictionary using a dot-separated path
    :param record: Record to extract the value from
    :param path: Dot-separated path to the value
    :param verbose: Print the path and record
    :return:  The value at the specified path or None if the path does not exist
    """
    # Split the path into keys
    for key in path.split('.'):
        if record is None or type(record) is not dict:
            # Print the path and record if the record is empty
            if verbose:
                print(f"Path: {path}, Record: {record}")
            # Return the default value if the record is empty
            if record is None:
                return default
            return record
        if key in record:
            record = record[key]
        else:
            return default

    return record


def parse_authors(record: dict) -> list:
    """
    Parse the authors from the record
    :param record: Record to extract the authors from
    :return: List of authors with their ID, name, and affiliation
    """
    # Check if the record is empty and it has author data
    if record is None:
        return []
    # In some cases the authors are stored in a separate field while by default they are stored in the 'dc:creator' field
    if 'authors' in record:
        authors = safe_get(record, 'authors.author', default=[])
    else:
        authors = safe_get(record, 'coredata.dc:creator', default=[])

    # Iterate over the authors and extract the author ID and name
    results = []
    for author in authors:
        # Check if the author has an affiliation
        if 'affiliation' not in author:
            affiliations = []
        # Affiliation can be a list or a dictionary - convert to a list
        elif type(author['affiliation']) is dict:
            affiliations = [author['affiliation']]
        else:
            affiliations = author['affiliation']

        # Process affiliations
        affiliation_ids = [{'id': affiliation['@id']} for affiliation in affiliations]
        try:
            results.append({
                'author_id': safe_get(author, '@auid'),
                'author_first_name': safe_get(author, 'ce:given-name'),
                'author_last_name': safe_get(author, 'ce:surname'),
                'author_indexed_name': safe_get(author, 'ce:indexed-name'),
                'author_initials': safe_get(author, 'ce:initials'),
                'author_affiliation_ids': affiliation_ids
            })
        except KeyError:
            print(f"Error for author: {author}")

    # Return the authors
    return results


def parse_keywords(record: dict) -> list:
    """
    Parse the keywords from the record
    :param record: Record to extract the keywords from
    :return: List of keywords
    """
    keywords = safe_get(record, 'authkeywords.author-keyword')
    # Check if the record is empty and it has keyword data
    if record is None or keywords is None:
        return []

    # Extract the keywords from the record
    results = []
    for keyword in record['authkeywords']['author-keyword']:
        results.append({
            'keyword': safe_get(keyword, '$')
        })
    # Return the keywords
    return results


def parse_references(record: dict) -> list:
    """
    Parse the references from the record
    :param record: Record to extract the references from
    :return: List of references with their ID and title
    """
    # Check if the record is empty and it has reference data
    references = safe_get(record, 'item.bibrecord.tail.bibliography.reference')
    if record is None or references is None:
        return []

    # In some cases there is only one reference and it is stored as a dictionary, convert to a list
    if type(references) is dict:
        references = [references]

    # Extract the references from the record
    results = []
    for reference in references:
        # Parse the reference title
        reference_title = safe_get(reference, 'ref-info.ref-title.ref-titletext')
        if reference_title is None:
            reference_title = safe_get(reference, 'ref-info.ref-text')
        # Append the reference to the list
        results.append({
            'reference_id': safe_get(reference, '@id'),
            'reference_title': reference_title
        })

    # Return the references
    return results


def parse_affiliations(record: dict) -> list:
    """
    Parse the affiliations from the record
    :param record: Record to extract the affiliations from
    :return: List of affiliations with their ID, name, city, and country
    """

    # Check if the record is empty and it has affiliation data
    affiliations = safe_get(record, 'affiliation')
    if record is None or affiliations is None:
        return []

    # In some cases there is only one affiliation and it is stored as a dictionary, convert to a list
    if type(affiliations) is dict:
        affiliations = [affiliations]

    # Extract the affiliations from the record
    results = []
    for affiliation in affiliations:
        # Parse the affiliation data
        affiliation_data = {
            'affiliation_name': safe_get(affiliation, 'affilname'),
            'affiliation_city': safe_get(affiliation, 'affiliation-city'),
            'affiliation_country': safe_get(affiliation, 'affiliation-country')
        }
        # Append the affiliation to the list
        results.append(affiliation_data)

    # Return the affiliations
    return results
