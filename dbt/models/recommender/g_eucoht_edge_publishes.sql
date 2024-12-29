WITH ref_g_included_authors AS (SELECT author_id
                                FROM {{ref("g_included_author") }}),
     ref_g_included_articles AS (SELECT article_id
                                 FROM {{ref("g_included_article") }}),
     ref_int_collaboration AS (SELECT c.author_id,
                                      c.article_id,
                                      c.article_publication_dt
                               FROM {{ref('int_collaboration') }} c
                                        INNER JOIN ref_g_included_authors au
                                                   ON c.author_id = au.author_id
                                        INNER JOIN ref_g_included_articles ar
                                                   ON c.article_id = ar.article_id)
SELECT author_id,
       article_id,
       TO_CHAR(article_publication_dt, 'YYYYMMDD') AS time
FROM ref_int_collaboration
GROUP BY author_id,
    article_id,
    article_publication_dt
