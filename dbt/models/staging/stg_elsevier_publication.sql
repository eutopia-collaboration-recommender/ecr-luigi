WITH src_elsevier_publication AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication') }})
SELECT publication_id,
       publication_eid,
       publication_doi,
       publication_title,
       publication_type,
       publication_abstract,
       publication_citation_count,
       publication_dt,
       publication_references,
       publication_keywords,
       a.value ->> 'author_id'           AS author_id,
       a.value ->> 'author_initials'     AS author_initials,
       a.value ->> 'author_last_name'    AS author_last_name,
       a.value ->> 'author_first_name'   AS author_first_name,
       a.value ->> 'author_indexed_name' AS author_indexed_name,
       af.value ->> 'id'                 AS affiliation_id
FROM src_elsevier_publication e,
     LATERAL JSONB_ARRAY_ELEMENTS(e.publication_authors) AS a,
     LATERAL JSONB_ARRAY_ELEMENTS(a.value -> 'author_affiliation_ids') AS af