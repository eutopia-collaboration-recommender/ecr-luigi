WITH src_elsevier_publication_affiliation AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
SELECT publication_id,
       publication_eid,
       publication_doi,
       publication_affiliation_id,
       a.value ->> 'author_id'           AS author_id,
       a.value ->> 'author_initials'     AS author_initials,
       a.value ->> 'author_last_name'    AS author_last_name,
       a.value ->> 'author_first_name'   AS author_first_name,
       a.value ->> 'author_indexed_name' AS author_indexed_name,
       af.value ->> 'id'                 AS affiliation_id
FROM src_elsevier_publication_affiliation e,
     LATERAL JSONB_ARRAY_ELEMENTS(e.publication_affiliations) AS a