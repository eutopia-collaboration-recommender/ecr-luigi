-- Get top 200 keywords
WITH top_k_keywords AS (SELECT article_keyword
                        FROM {{ ref('fct_article_keyword') }}
                        GROUP BY article_keyword
                        ORDER BY COUNT(DISTINCT article_id) DESC
                        LIMIT 200)
   , article_keyword AS (SELECT m.article_id,
                                a.article_publication_dt,
                                m.article_keyword,
                                m.research_area_code,
                                EXTRACT(YEAR FROM a.article_publication_dt) AS article_publication_year
                         FROM {{ ref('fct_article_keyword') }} m
                                  INNER JOIN {{ ref('g_included_article') }} a
                                             ON m.article_id = a.article_id)
-- Filter article keywords with top 200 keywords
   , top_k_article_keywords AS (SELECT m.*
                                FROM article_keyword m
                                         INNER JOIN top_k_keywords k
                                                    ON m.article_keyword = k.article_keyword)
   , top_k_article_keyword_trends AS (SELECT m.article_id,
                                             m.article_publication_dt,
                                             m.article_keyword,
                                             m.research_area_code,
                                             t.article_publication_year - m.article_publication_year AS kpi_year,
                                             t.publication_count_yoy_3yr_rolling_avg_diff            AS keyword_popularity_index
                                      FROM top_k_article_keywords m
                                               INNER JOIN {{ ref('fct_article_keyword_trend') }} t
                                                          ON m.article_keyword = t.article_keyword
                                                              AND m.research_area_code = t.research_area_code
                                                              AND t.article_publication_year
                                                                 BETWEEN m.article_publication_year - 5 AND m.article_publication_year - 1)
SELECT article_id,
       article_publication_dt,
       article_keyword,
       research_area_code,
       kpi_year,
       keyword_popularity_index
FROM top_k_article_keyword_trends