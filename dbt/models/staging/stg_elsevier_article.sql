WITH ref_stg_elsevier_publication AS (SELECT *
                                      FROM {{ ref('stg_elsevier_publication') }})
SELECT DISTINCT article_id,
                article_title,
                article_abstract,
                article_keywords,
                article_publication_dt
FROM ref_stg_elsevier_publication