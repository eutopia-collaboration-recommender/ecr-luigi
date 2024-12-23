WITH ref_int_collaboration AS (SELECT *
                               FROM {{ ref('int_collaboration') }}),
     ref_int_author_experience_by_article AS (SELECT *
                                              FROM {{ ref('int_author_experience_by_article') }})

SELECT c.article_id,
       c.author_id,
       c.research_area_code,
       c.institution_id,
       c.article_publication_dt,
       c.is_eutopia_collaboration,
       c.is_single_author_collaboration,
       c.is_internal_collaboration,
       c.is_external_collaboration,
       c.has_new_author_collaboration,
       c.has_new_institution_collaboration,
       a.article_count AS author_article_count
FROM ref_int_collaboration c
         LEFT JOIN ref_int_author_experience_by_article a
                   ON c.article_id = a.article_id
                       AND c.author_id = a.author_id