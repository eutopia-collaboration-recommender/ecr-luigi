WITH ref_int_collaboration AS (SELECT *
                               FROM {{ ref('int_collaboration') }})
SELECT c.author_id,
       c.article_id,
       COUNT(DISTINCT c2.article_id) AS article_count
FROM ref_int_collaboration c
         INNER JOIN ref_int_collaboration c2
                    ON c.author_id = c2.author_id
                        AND c2.article_publication_dt < c.article_publication_dt
GROUP BY c.author_id,
         c.article_id