WITH src_crossref_publication AS (SELECT *
                                  FROM {{ source('lojze', 'crossref_publication_parsed') }}),
     ref_crossref_publication_reference AS (SELECT *
                                            FROM {{ ref('stg_crossref_publication_top_3_reference') }})
SELECT DISTINCT p.article_doi,
                p.article_publisher,
                p.article_title,
                p.article_short_title,
                p.article_subtitle,
                p.article_original_title,
                p.article_container_title,
                p.article_short_container_title,
                p.article_abstract,
                p.article_publication_dt,
                p.article_referenced_by_count AS article_citation_count,
                r.article_references
FROM src_crossref_publication p
         LEFT JOIN ref_crossref_publication_reference r
                   ON r.article_doi = p.article_doi