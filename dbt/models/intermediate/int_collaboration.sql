WITH ref_stg_mart_collaboration AS (SELECT *
                                    FROM {{ ref('stg_mart_collaboration') }}),
     ref_stg_mart_collaboration_by_article AS (SELECT *
                                               FROM {{ ref('stg_mart_collaboration_by_article') }}),
     ref_stg_article_research_area AS (SELECT *
                                       FROM {{ ref('stg_article_research_area') }}),
     ref_stg_collaboration_novelty_metadata AS (SELECT *
                                                FROM {{ ref('stg_collaboration_novelty_metadata') }}),
     max_author_sequence_by_article AS (SELECT article_id,
                                               MAX(author_sequence) AS max_author_sequence
                                        FROM ref_stg_mart_collaboration
                                        GROUP BY article_id)
SELECT c.article_id,
       c.author_id,
       COALESCE(r.research_area_code, 'n/a') AS research_area_code,
       c.institution_id,
       CASE
           WHEN c.author_sequence = 1 AND m.max_author_sequence = 1 THEN 'singleAuthor'
           WHEN c.author_sequence = 1 THEN 'firstAuthor'
           WHEN c.author_sequence = m.max_author_sequence THEN 'lastAuthor'
           WHEN c.author_sequence IS NOT NULL THEN 'middleAuthor'
           ELSE 'n/a' END                    AS author_sequence_type,
       a.article_publication_dt,
       a.is_eutopia_collaboration,
       a.is_single_author_collaboration,
       a.is_internal_collaboration,
       a.is_external_collaboration,
       cn.has_new_author_collaboration,
       cn.has_new_institution_collaboration
FROM ref_stg_mart_collaboration c
         INNER JOIN ref_stg_mart_collaboration_by_article a
                    ON c.article_id = a.article_id
         LEFT JOIN ref_stg_article_research_area r
                   ON c.article_id = r.article_id
         LEFT JOIN ref_stg_collaboration_novelty_metadata cn
                   ON c.article_id = cn.article_id
                       AND c.author_id = cn.author_id
                       AND c.institution_id = cn.institution_id
         LEFT JOIN max_author_sequence_by_article m
                   ON c.article_id = m.article_id
WHERE article_publication_dt >= '2000-01-01'