WITH ref_stg_mart_els_core_collaboration AS (SELECT DISTINCT article_id
                                             FROM {{ ref('stg_mart_els_core_collaboration') }}),
     ref_stg_elsevier_publication_top_3_references AS (SELECT article_id,
                                                              article_references
                                                       FROM {{ ref('stg_elsevier_publication_top_3_reference') }}),
     ref_stg_elsevier_article AS (SELECT c.article_id,
                                         c.article_doi,
                                         c.article_eid,
                                         c.article_title,
                                         c.article_journal,
                                         c.article_abstract,
                                         r.article_references,
                                         c.article_publication_dt
                                  FROM {{ ref('stg_elsevier_article') }} c
                                           LEFT JOIN ref_stg_elsevier_publication_top_3_references r
                                                     ON c.article_id = r.article_id)
SELECT m.article_id,
       e.article_doi,
       e.article_eid,
       e.article_title,
       e.article_journal AS article_journal_title,
       e.article_abstract,
       e.article_references,
       e.article_publication_dt
FROM ref_stg_mart_els_core_collaboration m
         LEFT JOIN ref_stg_elsevier_article e
                   ON m.article_id = e.article_id