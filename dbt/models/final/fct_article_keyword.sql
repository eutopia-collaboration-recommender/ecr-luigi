WITH ref_stg_article_keywords_flattened AS (SELECT *
                                            FROM {{ ref('stg_article_keywords_flattened') }})
SELECT article_id,
       article_keyword
FROM ref_stg_article_keywords_flattened