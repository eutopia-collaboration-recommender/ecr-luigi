WITH src_elsevier_publication_affiliation AS (SELECT *
                                              FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
   , elsevier_affiliation AS (SELECT REPLACE(e.publication_id, 'SCOPUS_ID:', '') AS article_id,
                                     e.publication_eid                           AS article_eid,
                                     e.publication_doi                           AS article_doi,
                                     e.publication_affiliation_id                AS affiliation_id,
                                     a.value ->> 'affiliation_name'              AS affiliation_name,
                                     a.value ->> 'affiliation_city'              AS affiliation_city,
                                     a.value ->> 'affiliation_country'           AS affiliation_country
                              FROM src_elsevier_publication_affiliation e,
                                   LATERAL JSONB_ARRAY_ELEMENTS(e.publication_affiliations) AS a)
SELECT article_id,
       article_eid,
       article_doi,
       affiliation_id,
       CONCAT_WS('_',
                 affiliation_id,
                 affiliation_name,
                 affiliation_city,
                 affiliation_country) AS affiliation_identifier,
       affiliation_name,
       affiliation_city,
       affiliation_country
FROM elsevier_affiliation