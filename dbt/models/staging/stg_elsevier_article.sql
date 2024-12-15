WITH src_elsevier_publication AS (SELECT *
                                      FROM {{ source('lojze', 'elsevier_publication_parsed') }})
SELECT DISTINCT article_id,
                article_title,
                article_abstract,
                article_keywords,
                article_publication_dt
FROM src_elsevier_publication