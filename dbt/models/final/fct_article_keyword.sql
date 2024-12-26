WITH ref_int_article_keyword AS (SELECT *
                                 FROM {{ ref('int_article_keyword') }})
SELECT article_id,
       article_keyword,
       research_area_code
FROM ref_int_article_keyword
