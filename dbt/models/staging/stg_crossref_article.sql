WITH src_crossref_publication AS (SELECT *
                                  FROM {{ source('lojze', 'crossref_publication_parsed') }})
SELECT DISTINCT article_doi,
                article_publisher,
                article_title,
                article_short_title,
                article_subtitle,
                article_original_title,
                article_container_title,
                article_short_container_title,
                article_abstract,
                article_publication_dt
FROM src_crossref_publication