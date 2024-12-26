WITH ref_stg_article_keywords_flattened AS (SELECT *
                                            FROM {{ ref('stg_article_keywords_flattened') }}),
     ref_stg_article_research_area AS (SELECT *
                                       FROM {{ ref('stg_article_research_area') }})
SELECT f.article_id,
       f.article_keyword,
       r.research_area_code
FROM ref_stg_article_keywords_flattened f
         LEFT JOIN ref_stg_article_research_area r
                   ON r.article_id = f.article_id
