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
                                  FROM {{ref('stg_article_language')}})
SELECT a.article_id,
       a.article_doi,
       a.article_eid,
       a.article_title,
       a.article_journal_title,
       a.article_publication_dt,
       k.article_keywords,
       l.article_language
FROM ref_stg_mart_article a
         LEFT JOIN ref_stg_article_keywords k
                   ON a.article_id = k.article_id
         LEFT JOIN ref_stg_article_language l
                   ON a.article_id = l.article_id