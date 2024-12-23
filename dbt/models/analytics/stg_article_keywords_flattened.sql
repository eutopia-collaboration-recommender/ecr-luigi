SELECT article_id,
       REPLACE(REPLACE(REPLACE(article_keyword, '”', ''), '“', ''), '"', '') AS article_keyword
FROM {{ ref('stg_article_keywords') }},
     LATERAL UNNEST(article_keywords_arr) AS article_keyword