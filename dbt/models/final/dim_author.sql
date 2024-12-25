SELECT author_id,
       COALESCE(author_name, 'n/a') AS author_name
FROM {{ ref('stg_mart_author') }}