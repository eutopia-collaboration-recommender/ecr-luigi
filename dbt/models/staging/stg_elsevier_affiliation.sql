WITH src_elsevier_publication_affiliation AS (SELECT *
                                              FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
SELECT replace(publication_id, 'SCOPUS_ID:', '') AS publication_id,
       publication_eid,
       publication_doi,
       publication_affiliation_id,
       a.value ->> 'affiliation_name'            AS affiliation_name,
       a.value ->> 'affiliation_city'            AS affiliation_city,
       a.value ->> 'affiliation_country'         AS affiliation_country
FROM src_elsevier_publication_affiliation e,
     LATERAL JSONB_ARRAY_ELEMENTS(e.publication_affiliations) AS a