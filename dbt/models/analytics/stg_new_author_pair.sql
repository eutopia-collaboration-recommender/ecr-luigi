SELECT article_id,
       author_id,
       co_author_id,
       is_new_author_pair
FROM {{ source('analitik', 'new_author_pair') }}