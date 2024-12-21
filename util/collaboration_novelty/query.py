import pandas as pd
import psycopg2

from util.postgres import query


def query_collaboration_n_batches(conn: psycopg2.extensions.connection,
                                  batch_size: int = 1000,
                                  min_year: int = 2000) -> int:
    """
    Query the number of batches to process.
    :param conn: Connection to the Postgres database
    :param batch_size: Batch size
    :param min_year: Minimum year to consider
    :return: Number of batches to process
    """

    query_text = f"""
        WITH collaboration_by_article
                 /* Get articles that have not been processed yet. */
                 AS (SELECT article_id,
                            article_publication_dt,
                            is_single_author_collaboration
                     FROM stg_collaboration_by_article),
             batch_of_articles
                 /* Get a batch of articles given a predefined N excluding sole author publications. */
                 AS (SELECT COUNT(DISTINCT c.article_id) AS n
                     FROM stg_mart_collaboration c
                              INNER JOIN collaboration_by_article ca USING (article_id)
                     WHERE ca.is_single_author_collaboration = FALSE
                       AND EXTRACT(YEAR FROM ca.article_publication_dt) >= {min_year})
        /* Get the number of batches */
        SELECT CEIL(n / {batch_size}) AS n_batches
        FROM batch_of_articles
    """

    df = query(conn=conn, query=query_text)

    return int(df.iloc[0, 0])


def query_collaboration_batch(conn: psycopg2.extensions.connection,
                              batch_size: int = 1000,
                              min_year: int = 2000) -> pd.DataFrame:
    """
    Query a batch of collaborations that has not yet been processed.
    :param conn: Connection to the Postgres database
    :param batch_size: Batch size
    :param min_year: Minimum year to consider
    :return: DataFrame with the publications for the specific year
    """

    query_text = f"""
        WITH loaded_articles AS (SELECT article_id
                                 FROM collaboration_novelty_index
                                 GROUP BY article_id),
             collaboration_by_article
                 /* Get articles that have not been processed yet. */
                 AS (SELECT article_id,
                            article_publication_dt,
                            is_single_author_collaboration
                     FROM stg_collaboration_by_article),
             batch_of_articles
                 /* Get a batch of articles given a predefined N excluding sole author publications. */
                 AS (SELECT ca.article_id, ca.article_publication_dt
                     FROM collaboration_by_article ca
                              LEFT JOIN loaded_articles la USING (article_id)
                     WHERE ca.is_single_author_collaboration = FALSE
                       AND EXTRACT(YEAR FROM ca.article_publication_dt) >= {min_year}
                       AND la.article_id IS NULL
                     GROUP BY ca.article_id, ca.article_publication_dt
                     ORDER BY ca.article_publication_dt ASC
                     LIMIT {batch_size})
        /* Get the number of batches */
        SELECT ba.article_id,
               ba.article_publication_dt,
               c.author_id,
               c.institution_id
        FROM batch_of_articles ba
                 INNER JOIN stg_mart_collaboration c USING (article_id)
    """

    return query(conn=conn, query=query_text)
