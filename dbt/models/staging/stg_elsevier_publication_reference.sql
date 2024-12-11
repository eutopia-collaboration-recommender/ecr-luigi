WITH ref_stg_elsevier_publication AS (SELECT *
                                      FROM {{ ref('stg_elsevier_publication') }})
SELECT p.article_id,
       r.value ->> 'reference_id'    AS reference_id,
       r.value ->> 'reference_title' AS reference_title
FROM ref_stg_elsevier_publication AS p,
     LATERAL JSONB_ARRAY_ELEMENTS(article_references) AS r