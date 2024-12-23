import itertools
import psycopg2
import sqlalchemy
import polars as pl

from typing import Tuple
from util.postgres import query


def query_collaboration_novelty_num_batches(conn: psycopg2.extensions.connection | sqlalchemy.engine.base.Connection,
                                            batch_size: int = 10000,
                                            min_year: int = 2000,
                                            max_authors: int = 40) -> int:
    """
    Query the number of batches to process.
    :param conn: Connection to the Postgres database
    :param batch_size: Batch size
    :param min_year: Minimum year to consider
    :param max_authors: Maximum number of authors to consider
    :return: Number of batches to process
    """

    query_text = f"""
        WITH collaboration_by_article
                 /* Get articles. */
                 AS (SELECT article_id,
                            article_publication_dt
                     FROM stg_mart_collaboration_by_article
                     WHERE EXTRACT(YEAR FROM article_publication_dt) >= {min_year}
                       AND NOT is_single_author_collaboration
                     GROUP BY article_id, article_publication_dt),
             articles_to_load
                 /* Get all the authors for each article and filter out articles with more than 40 authors */
                 AS (SELECT article_id,
                            article_publication_dt
                     FROM collaboration_by_article ba
                              INNER JOIN stg_mart_collaboration c USING (article_id)
                     GROUP BY article_id, article_publication_dt
                     HAVING COUNT(DISTINCT author_id) <= {max_authors}),
             num_articles
                 /* Get the number of articles */
                 AS (SELECT COUNT(DISTINCT article_id) AS n
                     FROM articles_to_load)
        /* Get the number of batches */
        SELECT CEIL(n / {batch_size}) AS n_batches
        FROM num_articles
    """

    df = query(conn=conn, query=query_text)

    return int(df.iloc[0, 0])


def query_collaboration_novelty_batch(conn: sqlalchemy.engine.base.Connection,
                                      ix_batch: int,
                                      batch_size: int = 10000,
                                      min_year: int = 2000,
                                      max_authors: int = 40) -> pl.DataFrame:
    """
    Query collaborations that have not yet been processed.
    :param conn: Connection to the Postgres database
    :param min_year: Minimum year to consider
    :param batch_size: Batch size
    :param ix_batch: Index of the batch
    :param max_authors: Maximum number of authors to consider
    :return: DataFrame with the publications along with authors and institutions
    """
    # Calculate the offset
    offset = ix_batch * batch_size

    query_text = f"""
        WITH collaboration_by_article
                 /* Get articles. */
                 AS (SELECT article_id,
                            article_publication_dt
                     FROM stg_mart_collaboration_by_article
                     WHERE EXTRACT(YEAR FROM article_publication_dt) >= {min_year}
                       AND NOT is_single_author_collaboration
                     GROUP BY article_id, article_publication_dt),
             articles_to_load
                 /* Get all the authors for each article and filter out articles with more than 40 authors */
                 AS (SELECT article_id,
                            article_publication_dt
                     FROM collaboration_by_article ba
                              INNER JOIN stg_mart_collaboration c USING (article_id)
                     GROUP BY article_id, article_publication_dt
                     HAVING COUNT(DISTINCT author_id) <= {max_authors}),
             batch_of_articles
                 /* Get a batch of articles given a predefined N */
                 AS (SELECT article_id,
                            article_publication_dt
                     FROM articles_to_load
                     ORDER BY article_publication_dt ASC
                     LIMIT {batch_size} OFFSET {offset})
        /* Get all the collaboration data */
        SELECT ba.article_id,
               ba.article_publication_dt,
               c.author_id,
               c.institution_id
        FROM batch_of_articles ba
                 INNER JOIN stg_mart_collaboration c USING (article_id)
    """

    return pl.read_database(connection=conn, query=query_text)


class CollaborationNoveltyGraphTuple:
    def __init__(self):
        self.G_i = dict()
        self.G_a = dict()

    def calculate_CNI(self,
                      old_author_collaborations: list,
                      new_author_collaborations: list,
                      old_institution_collaborations: list,
                      new_institution_collaborations: list) -> float:
        """
        Calculate the Collaboration Novelty Index (CNI)
        :param old_author_collaborations: List of old author collaborations
        :param new_author_collaborations: List of new author collaborations
        :param old_institution_collaborations: List of old institution collaborations
        :param new_institution_collaborations: List of new institution collaborations
        :return: Collaboration Novelty Index
        """
        # Combine the new and old author and institution pairs
        author_pairs = new_author_collaborations + old_author_collaborations
        institution_pairs = new_institution_collaborations + old_institution_collaborations

        # Calculate size adjustment factor
        S_old = len(old_author_collaborations)
        S_a = 1 / (1 + S_old)

        # Calculate new author pair factor
        N_aa = sum(
            1 / (1 + self.G_a.get((a1, a2), 0))
            for (a1, a2) in author_pairs)

        # Calculate new institution pair factor
        N_ii = sum(
            1 / (1 + self.G_i.get((i1, i2), 0))
            for (i1, i2) in institution_pairs)

        # Calculate collaboration novelty index
        CNI = N_aa * (1 + N_ii) * S_a

        return CNI

    def update_graph(self,
                     graph: str,
                     items: pl.Series) -> Tuple[list, list]:
        """
        Update the author graph
        :param authors: DataFrame with authors
        :return: New and old author collaborations
        """
        if graph == 'author':
            G = self.G_a
        elif graph == 'institution':
            G = self.G_i
        else:
            raise ValueError(f"Graph {graph} not found. Please use 'author' or 'institution'.")

        new_item_collaborations, old_item_collaborations = list(), list()
        # Iterate over all the pairs of authors
        for item_1, item_2 in itertools.combinations(items, 2):
            try:
                # If the pair of authors is in the author collaboration history, add it to the old authors and increment the weight by 1
                if (item_1, item_2) in G:
                    G[item_1, item_2] += 1
                    old_item_collaborations.append((item_1, item_2))
                # If the pair of authors is not in the author collaboration history, add it to the new authors with a weight of 1
                else:
                    G[(item_1, item_2)] = 1
                    new_item_collaborations.append((item_1, item_2))
            except KeyError:
                # If the edge does not exist, add it to the graph. This should not happen, but it is a safety measure.
                G[(item_1, item_2)] = 1
                print(
                    f"Edge {item_1}-{item_2} not found in the graph even though it should be there. Added it with weight 1.")
                # Add to new authors
                new_item_collaborations.append((item_1, item_2))
            except ValueError:
                # If the vertices do not exist, add them to the graph. This will happen in first iterations.
                G[(item_1, item_2)] = 1

        return old_item_collaborations, new_item_collaborations

    def update(self, df: pl.DataFrame) -> Tuple[dict, list[dict]]:
        """
        Update the collaboration novelty graph tuple with the new article
        :param df: DataFrame with the article metadata
        :return: Collaboration novelty index and metadata
        """
        # Get authors and institutions
        authors = df['author_id'].unique()
        institutions = df['institution_id'].unique()

        # Update the institutions graph
        new_institution_collaborations, old_institution_collaborations = self.update_graph(
            graph='institution',
            items=institutions)

        # Update the authors graph
        old_author_collaborations, new_author_collaborations = self.update_graph(
            graph='author',
            items=authors
        )

        # Calculate the Collaboration Novelty Index
        CNI = self.calculate_CNI(
            old_author_collaborations=old_author_collaborations or [],
            new_author_collaborations=new_author_collaborations or [],
            old_institution_collaborations=old_institution_collaborations or [],
            new_institution_collaborations=new_institution_collaborations or []
        )

        # Enrich the original DataFrame with the collaboration novelty metadata

        # Add the has_new_author_collaboration column
        new_authors_set = set(itertools.chain(*new_author_collaborations))
        df = df.with_columns(
            has_new_author_collaboration=pl.col('author_id').is_in(new_authors_set)
        )

        # Add the has_new_institution_collaboration column
        new_institutions_set = set(itertools.chain(*new_institution_collaborations))
        df = df.with_columns(
            has_new_institution_collaboration=pl.col('institution_id').is_in(new_institutions_set)
        )
        # Transform to Pandas
        metadata = df.to_dicts()

        CNI_obj = dict(
            article_id=metadata[0]['article_id'],
            collaboration_novelty_index=CNI
        )
        return CNI_obj, metadata
