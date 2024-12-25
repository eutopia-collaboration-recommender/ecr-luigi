SELECT article_keyword,
       year AS article_publication_year,
       publication_count
FROM {{ source('analitik', 'article_keyword_trend') }}