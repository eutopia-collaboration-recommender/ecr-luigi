WITH src_elsevier_publication_affiliation AS (SELECT *
                                              FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
SELECT replace(publication_id, 'SCOPUS_ID:', '') AS article_id,
       publication_eid                           AS article_eid,
       publication_doi                           AS article_doi,
       publication_affiliation_id                AS affiliation_id,
       a.value ->> 'affiliation_name'            AS affiliation_name,
       a.value ->> 'affiliation_city'            AS affiliation_city,
       a.value ->> 'affiliation_country'         AS affiliation_country
FROM src_elsevier_publication_affiliation e,
     LATERAL JSONB_ARRAY_ELEMENTS(e.publication_affiliations) AS a