WITH ref_stg_mart_article AS (SELECT article_id,
                                     article_doi,
                                     article_eid,
                                     article_title,
                                     article_journal_title,
                                     article_publication_dt
                              FROM {{ref('stg_mart_article')}}),
     ref_stg_article_keywords AS (SELECT article_id,
                                         article_keywords_arr::TEXT AS article_keywords
                                  FROM {{ref('stg_article_keywords')}}),
     ref_stg_article_language AS (SELECT article_id,
                                         article_language
                                  FROM {{ref('stg_article_language')}}),
     ref_stg_mart_collaboration_by_article AS (SELECT article_id,
                                                      is_single_author_collaboration,
                                                      is_internal_collaboration,
                                                      is_external_collaboration,
                                                      is_eutopia_collaboration
                                               FROM {{ref('stg_mart_collaboration_by_article')}})
SELECT a.article_id,
       COALESCE(a.article_doi, 'n/a')                    AS article_doi,
       COALESCE(a.article_eid, 'n/a')                    AS article_eid,
       COALESCE(a.article_title, 'n/a')                  AS article_title,
       COALESCE(a.article_journal_title, 'n/a')          AS article_journal_title,
       a.article_publication_dt,
       COALESCE(k.article_keywords, 'n/a')               AS article_keywords,
       COALESCE(l.article_language, 'n/a')               AS article_language,
       COALESCE(c.is_single_author_collaboration, FALSE) AS is_single_author_collaboration,
       COALESCE(c.is_internal_collaboration, FALSE)      AS is_internal_collaboration,
       COALESCE(c.is_external_collaboration, FALSE)      AS is_external_collaboration,
       COALESCE(c.is_eutopia_collaboration, FALSE)       AS is_eutopia_collaboration
FROM ref_stg_mart_article a
         LEFT JOIN ref_stg_article_keywords k
                   ON a.article_id = k.article_id
         LEFT JOIN ref_stg_article_language l
                   ON a.article_id = l.article_id
         LEFT JOIN ref_stg_mart_collaboration_by_article c
                   ON a.article_id = c.article_id
