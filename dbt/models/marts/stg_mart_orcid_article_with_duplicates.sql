WITH ref_stg_mart_orcid_collaboration AS (SELECT DISTINCT article_id,
                                                          article_doi
                                          FROM {{ ref("stg_mart_orcid_collaboration_with_duplicates") }}),
     ref_stg_orcid_article AS (SELECT article_id,
                                      article_doi,
                                      article_eid,
                                      article_title,
                                      article_journal_title,
                                      article_publication_dt
                               FROM {{ ref('stg_orcid_article') }}),
     ref_stg_crossref_publication_top_3_references AS (SELECT article_doi,
                                                              article_references
                                                       FROM {{ ref('stg_crossref_publication_top_3_reference') }}),
     ref_stg_crossref_article AS (SELECT c.article_doi,
                                         c.article_title,
                                         c.article_container_title,
                                         c.article_abstract,
                                         c.article_publication_dt,
                                         c.article_citation_count,
                                         r.article_references
                                  FROM {{ ref('stg_crossref_article') }} c
                                           LEFT JOIN ref_stg_crossref_publication_top_3_references r
                                                     ON c.article_doi = r.article_doi)
SELECT m.article_id,
       COALESCE(m.article_doi, o.article_eid, o.article_doi, m.article_id) AS new_article_id,
       m.article_doi,
       o.article_eid,
       COALESCE(o.article_title, c.article_title)                          AS article_title,
       COALESCE(o.article_journal_title, c.article_container_title)        AS article_journal_title,
       c.article_abstract,
       c.article_references,
       GREATEST(c.article_citation_count, 0)     AS article_citation_count,
       LEAST(o.article_publication_dt, c.article_publication_dt)           AS article_publication_dt
FROM ref_stg_mart_orcid_collaboration m
         LEFT JOIN ref_stg_orcid_article o
                   ON m.article_id = o.article_id
         LEFT JOIN ref_stg_crossref_article c
                   ON m.article_doi = c.article_doi
