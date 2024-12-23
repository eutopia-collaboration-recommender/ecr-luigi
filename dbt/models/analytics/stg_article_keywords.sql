SELECT article_id,
       article_keywords_arr
FROM {{ source('analitik', 'article_keywords') }}