WITH ref_stg_article_keyword_trend AS (SELECT *
                                       FROM {{ ref('stg_article_keyword_trend') }})
SELECT article_keyword,
       article_publication_year,
       publication_count
FROM ref_stg_article_keyword_trend