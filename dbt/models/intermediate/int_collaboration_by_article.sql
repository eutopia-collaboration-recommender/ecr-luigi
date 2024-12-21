WITH ref_stg_mart_collaboration AS (SELECT article_id,
                                           author_id,
                                           institution_id
                                    FROM {{ ref('stg_mart_collaboration') }}),
     ref_stg_mart_article AS (SELECT article_id,
                                     article_citation_count,
                                     article_publication_dt
                              FROM {{ ref('stg_mart_article') }}),
     is_single_author_collaboration AS (SELECT article_id,
                                               COUNT(DISTINCT author_id) = 1 AS is_single_author_collaboration
                                        FROM ref_stg_mart_collaboration
                                        GROUP BY article_id),
     is_internal_collaboration AS (SELECT article_id,
                                          COUNT(DISTINCT author_id) > 1
                                              AND COUNT(DISTINCT institution_id) = 1 AS is_internal_collaboration
                                   FROM ref_stg_mart_collaboration
                                   GROUP BY article_id),
     is_external_collaboration AS (SELECT article_id,
                                          COUNT(DISTINCT author_id) > 1
                                              AND COUNT(DISTINCT institution_id) > 1 AS is_external_collaboration
                                   FROM ref_stg_mart_collaboration
                                   GROUP BY article_id),
     is_eutopia_collaboration AS (SELECT article_id,
                                         COUNT(DISTINCT author_id) > 1
                                             AND COUNT(DISTINCT institution_id) > 1 AS is_eutopia_collaboration
                                  FROM ref_stg_mart_collaboration
                                  WHERE institution_id NOT IN ('n/a', 'OTHER')
                                  GROUP BY article_id)
SELECT a.article_id,
       a.article_publication_dt,
       a.article_citation_count,
       COALESCE(single_author.is_single_author_collaboration, FALSE) AS is_single_author_collaboration,
       COALESCE(internal.is_internal_collaboration, FALSE)           AS is_internal_collaboration,
       COALESCE(external.is_external_collaboration, FALSE)           AS is_external_collaboration,
       COALESCE(eutopia.is_eutopia_collaboration, FALSE)             AS is_eutopia_collaboration
FROM ref_stg_mart_article a
         LEFT JOIN is_single_author_collaboration AS single_author
                   ON a.article_id = single_author.article_id
         LEFT JOIN is_internal_collaboration AS internal
                   ON a.article_id = internal.article_id
         LEFT JOIN is_external_collaboration AS external
                   ON a.article_id = external.article_id
         LEFT JOIN is_eutopia_collaboration AS eutopia
                   ON a.article_id = eutopia.article_id