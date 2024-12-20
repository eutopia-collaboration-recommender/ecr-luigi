WITH src_elsevier_publication AS (SELECT article_id,
                                         reference_id,
                                         reference_title,
                                         ROW_NUMBER() OVER (PARTITION BY article_id) AS rn
                                  FROM {{ ref('stg_elsevier_publication_reference') }}
                                  WHERE reference_title IS NOT NULL)
SELECT article_id,
       STRING_AGG(reference_title, ', ') AS article_references
FROM src_elsevier_publication
WHERE rn <= 3
GROUP BY article_id