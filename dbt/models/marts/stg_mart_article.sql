WITH ref_stg_mart_els_core_article AS (SELECT article_id,
                                              article_doi,
                                              article_eid,
                                              article_title,
                                              article_journal_title,
                                              article_abstract,
                                              article_references,
                                              article_publication_dt
                                       FROM {{ ref('stg_mart_els_core_article') }}),
     ref_stg_mart_els_enh_article AS (SELECT article_id,
                                             article_doi,
                                             article_eid,
                                             article_title,
                                             article_journal_title,
                                             article_abstract,
                                             article_references,
                                             article_publication_dt
                                      FROM {{ ref('stg_mart_els_enh_article') }}),
     ref_stg_mart_orcid_article AS (SELECT article_id,
                                           article_doi,
                                           article_eid,
                                           article_title,
                                           article_journal_title,
                                           article_abstract,
                                           article_references,
                                           article_publication_dt
                                    FROM {{ ref('stg_mart_orcid_article') }}),
     merged AS (SELECT *
                FROM ref_stg_mart_els_core_article
                UNION ALL
                SELECT *
                FROM ref_stg_mart_els_enh_article
                UNION ALL
                SELECT *
                FROM ref_stg_mart_orcid_article)
SELECT DISTINCT article_id,
                FIRST_VALUE(article_doi) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_doi IS NOT NULL THEN 1 ELSE 0 END DESC, article_doi DESC)                       AS article_doi,
                FIRST_VALUE(article_eid) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_eid IS NOT NULL THEN 1 ELSE 0 END DESC, article_eid DESC)                       AS article_eid,
                FIRST_VALUE(article_title) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_title IS NOT NULL THEN 1 ELSE 0 END DESC, article_title DESC)                   AS article_title,
                FIRST_VALUE(article_journal_title) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_journal_title IS NOT NULL THEN 1 ELSE 0 END DESC, article_journal_title DESC)   AS article_journal_title,
                FIRST_VALUE(article_abstract) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_abstract IS NOT NULL THEN 1 ELSE 0 END DESC, article_abstract DESC)             AS article_abstract,
                FIRST_VALUE(article_references) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_references IS NOT NULL THEN 1 ELSE 0 END DESC, article_references DESC)         AS article_references,
                FIRST_VALUE(article_publication_dt) OVER (PARTITION BY article_id
                    ORDER BY CASE WHEN article_publication_dt IS NOT NULL THEN 1 ELSE 0 END DESC, article_publication_dt DESC) AS article_publication_dt
FROM merged