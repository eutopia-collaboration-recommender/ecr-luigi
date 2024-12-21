WITH src_elsevier_publication AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication_parsed') }}),
     ref_elsevier_publication_reference AS (SELECT *
                                            FROM {{ ref('stg_elsevier_publication_top_3_reference') }})

SELECT DISTINCT p.article_id,
                p.article_eid,
                p.article_doi,
                p.article_title,
                p.article_journal,
                p.article_abstract,
                p.article_keywords,
                p.article_citation_count,
                p.article_publication_dt,
                r.article_references
FROM src_elsevier_publication p
         LEFT JOIN ref_elsevier_publication_reference r
                   ON r.article_id = p.article_id