WITH ref_stg_new_author_pair AS (SELECT *
                                 FROM {{ ref('stg_new_author_pair') }})
SELECT article_id,
       author_id,
       co_author_id,
       is_new_author_pair
FROM ref_stg_new_author_pair