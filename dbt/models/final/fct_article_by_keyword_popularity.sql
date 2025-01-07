WITH articles_by_type AS (SELECT article_id,
                                 EXTRACT(YEAR FROM article_publication_dt) AS article_publication_year,
                                 CASE
                                     WHEN is_new_collaboration THEN 'new'
                                     ELSE 'existing' END                   AS collaboration_type
                          FROM {{ ref('fct_article') }}
                          WHERE is_single_author_collaboration = FALSE),
     article_keyword AS (SELECT fk.article_id,
                                fk.article_keyword,
                                fk.research_area_code,
                                d.article_publication_year,
                                d.collaboration_type
                         FROM {{ ref('fct_article_keyword') }} fk
                                  INNER JOIN articles_by_type d
                                             ON fk.article_id = d.article_id),
     article_keyword_trend AS (SELECT DISTINCT article_keyword,
                                               research_area_code,
                                               article_publication_year,
                                               is_trend_positive,
                                               publication_count_yoy_3yr_rolling_avg_diff AS popularity_index
                               FROM {{ ref('fct_article_keyword_trend') }}),
     final AS (SELECT a.article_id,
                      a.collaboration_type,
                      COUNT(DISTINCT CASE WHEN b.is_trend_positive THEN a.article_keyword END) AS keyword_popularity,
                      SUM(popularity_index)                                AS keyword_popularity_index
               FROM article_keyword a
                        INNER JOIN article_keyword_trend b
                                   ON a.article_keyword = b.article_keyword
                                       AND a.research_area_code = b.research_area_code
                                       AND a.article_publication_year = b.article_publication_year
               GROUP BY a.article_id,
                        a.collaboration_type)
SELECT article_id,
       collaboration_type,
       keyword_popularity,
       keyword_popularity_index
FROM final