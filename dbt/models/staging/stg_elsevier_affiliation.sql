WITH src_elsevier_publication_affiliation AS (SELECT *
                                              FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
   , elsevier_affiliation AS (SELECT REPLACE(e.publication_id, 'SCOPUS_ID:', '') AS article_id,
                                     e.publication_eid                           AS article_eid,
                                     e.publication_doi                           AS article_doi,
                                     e.publication_affiliation_id                AS affiliation_id
                              FROM src_elsevier_publication_affiliation e)
SELECT article_id,
       article_eid,
       article_doi,
       affiliation_id
FROM elsevier_affiliation