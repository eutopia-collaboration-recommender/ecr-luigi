WITH src_elsevier_publication AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication') }})
SELECT p.publication_id              AS article_id,
       r.value ->> 'reference_id'    AS reference_id,
       r.value ->> 'reference_title' AS reference_title
FROM src_elsevier_publication AS p,
     LATERAL JSONB_ARRAY_ELEMENTS(publication_references) AS r