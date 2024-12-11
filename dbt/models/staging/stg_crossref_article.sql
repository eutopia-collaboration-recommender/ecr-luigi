WITH ref_stg_crossref_publication AS (SELECT *
                                      FROM {{ ref('stg_crossref_publication') }})
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
FROM ref_stg_crossref_publication