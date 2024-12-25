WITH ref_stg_mart_article AS (SELECT article_id,
                                     article_citation_count,
                                     EXTRACT(YEAR FROM article_publication_dt) AS publication_year
                              FROM {{ ref('stg_mart_article') }}),
     ref_stg_article_research_area AS (SELECT *
                                       FROM {{ ref('stg_article_research_area') }}),
     article_citation_by_research_area AS (SELECT research_area_code,
                                                  publication_year,
                                                  SUM(a.article_citation_count) AS article_citation_count
                                           FROM ref_stg_mart_article a
                                                    LEFT JOIN ref_stg_article_research_area r
                                                              ON a.article_id = r.article_id
                                           GROUP BY research_area_code,
                                                    publication_year),
     article_by_research_area AS (SELECT a.article_id,
                                         a.article_citation_count,
                                         a.publication_year,
                                         r.research_area_code
                                  FROM ref_stg_mart_article a
                                           LEFT JOIN ref_stg_article_research_area r
                                                     ON a.article_id = r.article_id)
SELECT a.article_id,
       a.article_citation_count,
       ROUND(CASE
                 WHEN c.article_citation_count IS NULL
                     OR c.article_citation_count = 0
                     OR c.article_citation_count = 1 THEN a.article_citation_count::NUMERIC
                 ELSE (a.article_citation_count / LOG(c.article_citation_count))::NUMERIC
                 END, 2) AS article_citation_normalized_count
FROM article_by_research_area a
         LEFT JOIN article_citation_by_research_area c
                   ON a.publication_year = c.publication_year
                       AND a.research_area_code = c.research_area_code
