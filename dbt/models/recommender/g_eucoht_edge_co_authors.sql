WITH ref_g_included_authors AS (SELECT author_id
                                FROM {{ ref("g_included_author") }}),
     ref_g_included_articles AS (SELECT article_id
                                 FROM {{ ref("g_included_article") }}),
     ref_int_collaboration AS (SELECT c.author_id,
                                      c.article_id,
                                      c.is_eutopia_collaboration,
                                      TO_CHAR(c.article_publication_dt, 'YYYYMMDD') AS time
                               FROM {{ ref('int_collaboration') }} c
                                        INNER JOIN ref_g_included_authors au
                                                   ON c.author_id = au.author_id
                                        INNER JOIN ref_g_included_articles ar
                                                   ON c.article_id = ar.article_id)
SELECT c1.author_id,
       c2.author_id                                   AS co_author_id,
       MIN(c1.time)                                   AS time,
       COUNT(DISTINCT CASE
                          WHEN c1.is_eutopia_collaboration = TRUE
                              THEN c1.article_id END) AS eutopia_collaboration_count
FROM ref_int_collaboration c1
         INNER JOIN ref_int_collaboration c2
                    ON c1.article_id = c2.article_id
                        AND c1.author_id <> c2.author_id
GROUP BY c1.author_id,
         c2.author_id
HAVING COUNT(DISTINCT c1.article_id) >= 5

