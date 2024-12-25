WITH ref_stg_mart_els_collaboration AS (SELECT article_id,
                                               author_id,
                                               institution_id,
                                               author_sequence
                                        FROM {{ ref("stg_mart_els_collaboration") }}),
     ref_stg_mart_orcid_collaboration AS (SELECT article_id,
                                                 author_id,
                                                 institution_id,
                                                 author_sequence
                                          FROM {{ ref('stg_mart_orcid_collaboration') }}),
     merged AS (SELECT *
                FROM ref_stg_mart_els_collaboration
                UNION ALL
                SELECT *
                FROM ref_stg_mart_orcid_collaboration),
     filtered_articles AS (SELECT article_id
                           FROM merged
                           GROUP BY article_id
                           HAVING COUNT(DISTINCT author_id) <= 40)
SELECT m.article_id,
       m.author_id,
       m.institution_id,
       m.author_sequence
FROM merged m
         INNER JOIN filtered_articles fa USING (article_id)