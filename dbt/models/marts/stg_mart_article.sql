WITH ref_stg_mart_els_article AS (SELECT article_id,
                                         article_doi,
                                         article_eid,
                                         article_title,
                                         article_journal_title,
                                         article_abstract,
                                         article_references,
                                         article_citation_count,
                                         article_publication_dt
                                  FROM {{ ref("stg_mart_els_article") }}),
     ref_stg_mart_orcid_article AS (SELECT article_id,
                                           article_doi,
                                           article_eid,
                                           article_title,
                                           article_journal_title,
                                           article_abstract,
                                           article_references,
                                           article_citation_count,
                                           article_publication_dt
                                    FROM {{ ref('stg_mart_orcid_article') }}),
     merged AS (SELECT *
                FROM ref_stg_mart_els_article
                UNION ALL
                SELECT *
                FROM ref_stg_mart_orcid_article),
     filtered_articles AS (SELECT DISTINCT article_id
                           FROM {{ ref('stg_mart_collaboration') }})
SELECT DISTINCT m.article_id,
                FIRST_VALUE(m.article_doi) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_doi IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_doi DESC)                       AS article_doi,
                FIRST_VALUE(m.article_eid) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_eid IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_eid DESC)                       AS article_eid,
                FIRST_VALUE(m.article_title) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_title IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_title DESC)                   AS article_title,
                FIRST_VALUE(m.article_journal_title) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_journal_title IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_journal_title DESC)   AS article_journal_title,
                FIRST_VALUE(m.article_abstract) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_abstract IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_abstract DESC)             AS article_abstract,
                FIRST_VALUE(m.article_references) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_references IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_references DESC)         AS article_references,
                FIRST_VALUE(m.article_citation_count) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_citation_count IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_citation_count DESC) AS article_citation_count,
                FIRST_VALUE(m.article_publication_dt) OVER (PARTITION BY m.article_id
                    ORDER BY CASE WHEN m.article_publication_dt IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_publication_dt DESC) AS article_publication_dt
FROM merged m
         INNER JOIN filtered_articles fa USING (article_id)