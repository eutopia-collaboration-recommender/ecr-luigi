WITH ref_int_article_keyword_trend AS (SELECT article_keyword,
                                              article_publication_year,
                                              publication_count
                                       FROM {{ ref('int_article_keyword_trend') }}),
     ref_int_article_keyword AS (SELECT article_id,
                                        article_keyword,
                                        research_area_code
                                 FROM {{ ref('int_article_keyword') }}),
     trend_by_year AS (SELECT t.article_keyword,
                              t.article_publication_year,
                              t.publication_count,
                              m.research_area_code
                       FROM ref_int_article_keyword_trend t
                                INNER JOIN ref_int_article_keyword m
                                           ON t.article_keyword = m.article_keyword),
     trend_research_area_by_year AS (SELECT research_area_code,
                                            article_publication_year,
                                            SUM(publication_count) AS research_area_publication_count
                                     FROM trend_by_year
                                     GROUP BY research_area_code, article_publication_year),
     trend_normalized_by_year AS (SELECT DISTINCT a.article_keyword,
                                                  a.article_publication_year,
                                                  a.publication_count,
                                                  a.research_area_code,
                                                  b.research_area_publication_count,
                                                  -- Normalize publication_count by research_area_publication_count
                                                  CASE
                                                      WHEN a.publication_count <= 1 THEN a.publication_count
                                                      ELSE DIV(a.publication_count::NUMERIC,
                                                               LOG(1 + b.research_area_publication_count)::NUMERIC)
                                                      END AS publication_normalized_count
                                  FROM trend_by_year a
                                           JOIN trend_research_area_by_year b
                                                ON a.research_area_code = b.research_area_code
                                                    AND a.article_publication_year = b.article_publication_year),

-- Add YoY = (current - LAG(current)) / LAG(current)
     trend_yoy_by_year AS (SELECT DISTINCT article_keyword,
                                           research_area_code,
                                           article_publication_year,
                                           publication_count,
                                           research_area_publication_count,
                                           publication_normalized_count,
                                           -- Year-over-year change for publication_normalized_count
                                           CASE
                                               WHEN LAG(publication_normalized_count)
                                                    OVER (PARTITION BY article_keyword, research_area_code
                                                        ORDER BY article_publication_year) = 0
                                                   OR
                                                    LAG(publication_normalized_count)
                                                    OVER (PARTITION BY article_keyword, research_area_code
                                                        ORDER BY article_publication_year) IS NULL
                                                   THEN NULL
                                               ELSE
                                                   (publication_normalized_count - LAG(publication_normalized_count)
                                                                                   OVER ( PARTITION BY article_keyword, research_area_code
                                                                                       ORDER BY article_publication_year))
                                                       /
                                                   LAG(publication_normalized_count) OVER (
                                                       PARTITION BY article_keyword, research_area_code
                                                       ORDER BY article_publication_year )
                                               END AS yoy
                           FROM trend_normalized_by_year),

--  Now we compute the YoY rolling average trend by year.
     trend_yoy_3yr_rolling_by_year AS (SELECT DISTINCT article_keyword,
                                                       research_area_code,
                                                       article_publication_year,
                                                       publication_count,
                                                       research_area_publication_count,
                                                       publication_normalized_count,
                                                       yoy,
                                                       AVG(yoy) OVER (
                                                           PARTITION BY article_keyword, research_area_code
                                                           ORDER BY article_publication_year
                                                           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                                                           ) AS yoy_3yr_rolling_avg
                                       FROM trend_yoy_by_year),
     trend_baseline_by_year AS (select research_area_code,
                                       article_publication_year,
                                       -- Median publication count
                                       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY publication_count)   AS publication_median_count,
                                       PERCENTILE_CONT(0.5)
                                       WITHIN GROUP (ORDER BY publication_normalized_count)             AS publication_normalized_median_count,
                                       -- Median YoY
                                       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY yoy)                 AS yoy_median,
                                       -- Median 3-year rolling average YoY
                                       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY yoy_3yr_rolling_avg) AS yoy_3yr_rolling_avg_median
                                from trend_yoy_3yr_rolling_by_year
                                GROUP BY research_area_code, article_publication_year),
     final AS (SELECT n.article_keyword,
                      n.research_area_code,
                      n.article_publication_year,
                      n.publication_count,
                      n.publication_normalized_count,
                      n.yoy                                                     AS publication_count_yoy,
                      n.yoy_3yr_rolling_avg                                     AS publication_count_yoy_3yr_rolling_avg,
                      b.publication_median_count                                AS publication_count_baseline,
                      b.publication_normalized_median_count                     AS publication_normalized_count_baseline,
                      b.yoy_median                                              AS publication_count_yoy_baseline,
                      b.yoy_3yr_rolling_avg_median                              AS publication_count_yoy_3yr_rolling_avg_baseline,
                      n.yoy_3yr_rolling_avg - b.yoy_3yr_rolling_avg_median      AS publication_count_yoy_3yr_rolling_avg_diff,
                      DIV((n.yoy_3yr_rolling_avg - b.yoy_3yr_rolling_avg_median)::NUMERIC,
                          (CASE
                               WHEN b.yoy_3yr_rolling_avg_median = 0 THEN 1
                               ELSE b.yoy_3yr_rolling_avg_median END)::NUMERIC) AS publication_count_yoy_3yr_rolling_avg_diff_pct,
                      CASE
                          WHEN n.yoy_3yr_rolling_avg IS NULL OR b.yoy_3yr_rolling_avg_median IS NULL THEN FALSE
                          WHEN n.yoy_3yr_rolling_avg > b.yoy_3yr_rolling_avg_median + 0.02 THEN TRUE
                          ELSE FALSE
                          END                                                   AS is_trend_positive
               FROM trend_yoy_3yr_rolling_by_year n
                        LEFT JOIN trend_baseline_by_year b
                                  ON n.research_area_code = b.research_area_code
                                      AND n.article_publication_year = b.article_publication_year)

SELECT article_keyword,
       research_area_code,
       article_publication_year,
       publication_count,
       publication_normalized_count,
       publication_count_yoy,
       publication_count_yoy_3yr_rolling_avg,
       publication_count_baseline,
       publication_normalized_count_baseline,
       publication_count_yoy_baseline,
       publication_count_yoy_3yr_rolling_avg_baseline,
       publication_count_yoy_3yr_rolling_avg_diff,
       publication_count_yoy_3yr_rolling_avg_diff_pct,
       is_trend_positive
FROM final
