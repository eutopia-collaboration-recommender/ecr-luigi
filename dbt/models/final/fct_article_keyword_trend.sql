WITH ref_int_article_keyword AS (SELECT DISTINCT article_keyword,
                                                 research_area_code
                                 FROM {{ref('int_article_keyword') }}),
     ref_int_article_keyword_trend AS (SELECT article_keyword,
                                              article_publication_year,
                                              SUM(publication_count) AS publication_count
                                       FROM {{ ref('int_article_keyword_trend') }}
                                       GROUP BY article_keyword, article_publication_year),
     publication_by_research_area AS (SELECT r.research_area_code,
                                             p.article_publication_year,
                                             SUM(p.publication_count) AS publication_count
                                      FROM ref_int_article_keyword_trend p
                                               INNER JOIN ref_int_article_keyword r
                                                          ON p.article_keyword = r.article_keyword
                                      GROUP BY r.research_area_code, p.article_publication_year),
     normalization_factor_by_research_area AS (SELECT research_area_code,
                                                      article_publication_year,
                                                      CASE
                                                          WHEN publication_count = 0 THEN 1
                                                          ELSE LOG(1 + publication_count) END AS normalization_factor
                                               FROM publication_by_research_area)
SELECT t.article_publication_year,
       t.article_keyword,
       t.publication_count,
       t.publication_count / n.normalization_factor AS publication_normalized_count
FROM ref_int_article_keyword_trend t
         LEFT JOIN ref_int_article_keyword k
                   ON t.article_keyword = k.article_keyword
         LEFT JOIN normalization_factor_by_research_area n
                   ON k.research_area_code = n.research_area_code
                       AND t.article_publication_year = n.article_publication_year
