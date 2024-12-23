SELECT author_id,
       author_name
FROM {{ ref('stg_mart_author') }}