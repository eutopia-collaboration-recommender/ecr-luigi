SELECT article_id,
       author_id,
       institution_id,
       BOOL_OR(has_new_author_collaboration)      AS has_new_author_collaboration,
       BOOL_OR(has_new_institution_collaboration) AS has_new_institution_collaboration
FROM {{ source('analitik', 'collaboration_novelty_metadata') }}
GROUP BY article_id,
         author_id,
         institution_id