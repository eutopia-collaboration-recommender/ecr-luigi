import itertools

import networkx as nx
import pandas as pd


def is_new_institution_collaboration(G: nx.Graph,
                                     authors_institution_1: str,
                                     authors_institution_2: str) -> bool:
    """
    Check if a pair of institutions have collaborated before. We consider that a pair of institutions have not
    collaborated before if there is no pair of authors from the pair of institutions that have collaborated before.
    :param G: Collaboration graph
    :param authors_institution_1: Authors from institution 1
    :param authors_institution_2: Authors from institution 2
    :return: True if the pair of institutions have not collaborated before, False otherwise.
    """
    for author_institution_1, author_institution_2 in itertools.product(authors_institution_1, authors_institution_2):
        # If the pair of authors have collaborated before, the pair of institutions have collaborated before
        if G.has_edge(author_institution_1, author_institution_2):
            # If the pair of institutions have collaborated before, the collaboration is not new
            return False

    # If there is no pair of authors from the pair of institutions that have collaborated before, the pair of
    # institutions have not collaborated before and the collaboration is new.
    # TODO: This is not the correct definition when authors have previously collaborated but were then affiliated with
    #  different institutions.
    return True


def is_new_author_collaboration(G: nx.Graph,
                                author_1: str,
                                author_2: str) -> bool:
    """
    Check if a pair of authors have collaborated before.
    :param G: Collaboration graph
    :param author_1: Author 1
    :param author_2: Author 2
    :return: True if the pair of authors have not collaborated before, False otherwise
    """
    return not G.has_edge(author_1, author_2)


def update_collaboration(G_a: nx.Graph,
                         G_i: nx.Graph,
                         diff: dict) -> None:
    """
    Update the collaboration history graphs with the new collaboration information.
    :param G_a: Collaboration author graph
    :param G_i: Collaboration institution graph
    :param diff: Difference between the collaboration history and the new publication
    :return: Updated author and institution collaboration history graphs
    """
    # Update the author collaboration history (for old authors)
    for author_tuple in diff['old_authors']:
        try:
            # Increment the number of collaborations attribute for the pair of authors
            G_a[author_tuple[0]][author_tuple[1]]['weight'] += 1
        except KeyError:
            G_a.add_edge(author_tuple[0], author_tuple[1], weight=1)

    # Update the author collaboration history (for new authors)
    for author_tuple in diff['new_authors']:
        # Add a new edge between the pair of authors and set the number of collaborations to 1
        G_a.add_edge(author_tuple[0], author_tuple[1], weight=1)

    # Update the institution collaboration history (for old institutions)
    for institution_tuple in diff['old_institutions']:
        try:
            # Increment the number of collaborations attribute for the pair of institutions
            G_i[institution_tuple[0]][institution_tuple[1]]['weight'] += 1
        except KeyError:
            G_i.add_edge(institution_tuple[0], institution_tuple[1], weight=1)
    # Update the institution collaboration history (for new institutions)
    for institution_tuple in diff['new_institutions']:
        # Add a new edge between the pair of institutions and set the number of collaborations to 1
        G_i.add_edge(institution_tuple[0], institution_tuple[1], weight=1)


def collaboration_difference_by_author(G: nx.Graph,
                                       authors: list) -> tuple:
    """
    Calculate the difference between the collaboration history and the new publication for authors.
    :param G: Collaboration graph
    :param authors: List of authors
    :return: Tuple of new and old authors lists
    """
    # Init the new and old authors lists
    new_authors, old_authors = list(), list()

    # Go through all the pairs of authors
    for author_1, author_2 in itertools.combinations(authors, 2):
        # If the pair of authors is not in the author collaboration history, add it to the new authors
        if is_new_author_collaboration(G, author_1, author_2):
            new_authors.append((author_1, author_2))
        else:
            old_authors.append((author_1, author_2))

    return new_authors, old_authors


def collaboration_difference_by_institution(G: nx.Graph,
                                            institutions: list,
                                            author_affiliations: pd.DataFrame) -> tuple:
    """
    Calculate the difference between the collaboration history and the new publication for institutions.
    :param G: Collaboration graph
    :param institutions: The list of institutions
    :param author_affiliations: The DataFrame with the author and institution pairs
    :return:
    """
    # Init the new and old institutions lists
    new_institutions, old_institutions = list(), list()

    # Go through all the pairs of institutions and authors
    for institution_1, institution_2 in itertools.combinations(institutions, 2):
        # Get full batch of authors from the pair of institutions
        authors_institution_1 = author_affiliations[author_affiliations['institution_id'] == institution_1][
            'author_id']
        authors_institution_2 = author_affiliations[author_affiliations['institution_id'] == institution_2][
            'author_id']

        # Check if the pair of institutions have collaborated before
        if is_new_institution_collaboration(G=G,
                                            authors_institution_1=authors_institution_1,
                                            authors_institution_2=authors_institution_2):
            new_institutions.append((institution_1, institution_2))
        else:
            old_institutions.append((institution_1, institution_2))

    return new_institutions, old_institutions


def collaboration_difference(G: nx.Graph,
                             author_affiliations: pd.DataFrame) -> dict:
    """
    Calculate the difference between the collaboration history and the new publication.
    :param G: Collaboration graph
    :param author_affiliations: List of author and institution pairs
    :return: Difference between the collaboration history and the new publication
    """
    # Get the authors and institutions
    authors = author_affiliations['author_id'].unique()
    institutions = author_affiliations['institution_id'].unique()
    # Authors
    new_authors, old_authors = collaboration_difference_by_author(G=G,
                                                                  authors=authors)

    # Institutions
    new_institutions, old_institutions = collaboration_difference_by_institution(
        G=G,
        institutions=institutions,
        author_affiliations=author_affiliations
    )

    # Return the difference between the collaboration history and the new publication
    return dict(
        new_authors=new_authors,
        old_authors=old_authors,
        new_institutions=new_institutions,
        old_institutions=old_institutions
    )
