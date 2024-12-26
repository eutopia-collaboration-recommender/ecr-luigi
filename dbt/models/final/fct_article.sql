WITH ref_stg_mart_collaboration_by_article AS (SELECT *
                                               FROM {{ ref('stg_mart_collaboration_by_article') }}),
     ref_stg_article_research_area AS (SELECT *
                                       FROM {{ ref('stg_article_research_area') }}),
     ref_int_article_citation_normalized AS (SELECT *
                                             FROM {{ ref('int_article_citation_normalized') }}),
     ref_stg_collaboration_novelty_index AS (SELECT *
                                             FROM {{ ref('stg_collaboration_novelty_index') }}),
     ref_int_collaboration AS (SELECT article_id,
                                      BOOL_OR(has_new_author_collaboration) OR
                                      BOOL_OR(has_new_institution_collaboration) AS is_new_collaboration
                               FROM {{ ref('int_collaboration') }}
                               GROUP BY article_id)
SELECT c.article_id,
       COALESCE(r.research_area_code, 'n/a')      AS research_area_code,
       n.article_citation_count,
       n.article_citation_normalized_count,
       c.article_publication_dt,
       c.is_eutopia_collaboration,
       c.is_single_author_collaboration,
       c.is_internal_collaboration,
       c.is_external_collaboration,
       ic.is_new_collaboration,
       COALESCE(i.collaboration_novelty_index, 0) AS collaboration_novelty_index
FROM ref_stg_mart_collaboration_by_article c
         LEFT JOIN ref_stg_article_research_area r
                   ON c.article_id = r.article_id
         LEFT JOIN ref_int_article_citation_normalized n
                   ON c.article_id = n.article_id
         LEFT JOIN ref_stg_collaboration_novelty_index i
                   ON c.article_id = i.article_id
         LEFT JOIN ref_int_collaboration ic
                   ON c.article_id = ic.article_id