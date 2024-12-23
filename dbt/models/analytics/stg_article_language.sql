SELECT article_id,
       article_language
FROM {{ source('analitik', 'article_language') }}