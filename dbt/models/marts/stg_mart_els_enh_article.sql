WITH ref_stg_mart_els_core_collaboration AS (SELECT DISTINCT article_id,
                                                             crossref_article_doi
                                             FROM {{ ref('stg_mart_els_enh_collaboration') }}),
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
                                                     ON c.article_id = r.article_id),
     ref_stg_crossref_publication_top_3_references AS (SELECT article_doi,
                                                              article_references
                                                       FROM {{ ref('stg_crossref_publication_top_3_reference') }}),
     ref_stg_crossref_article AS (SELECT c.article_doi,
                                         c.article_title,
                                         c.article_container_title,
                                         c.article_abstract,
                                         c.article_publication_dt,
                                         r.article_references
                                  FROM {{ ref('stg_crossref_article') }} c
                                           LEFT JOIN ref_stg_crossref_publication_top_3_references r
                                                     ON c.article_doi = r.article_doi)
SELECT m.article_id,
       e.article_doi,
       e.article_eid,
       COALESCE(e.article_title, c.article_title)                AS article_title,
       COALESCE(e.article_journal, c.article_container_title)    AS article_journal_title,
       COALESCE(e.article_abstract, c.article_abstract)          AS article_abstract,
       COALESCE(e.article_references, c.article_references)      AS article_references,
       LEAST(e.article_publication_dt, c.article_publication_dt) AS article_publication_dt
FROM ref_stg_mart_els_core_collaboration m
         LEFT JOIN ref_stg_elsevier_article e
                   ON m.article_id = e.article_id
         LEFT JOIN ref_stg_crossref_article c
                   ON m.crossref_article_doi = c.article_doi