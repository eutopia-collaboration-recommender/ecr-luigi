WITH src_crossref_publication AS (SELECT *
                                  FROM {{ source('lojze', 'crossref_publication_parsed') }})
SELECT DISTINCT p.article_doi,
                p.author_sequence,
                p.author_sequence_index,
                p.author_orcid
FROM src_crossref_publication p