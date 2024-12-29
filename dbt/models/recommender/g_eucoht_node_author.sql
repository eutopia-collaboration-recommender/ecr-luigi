SELECT author_id,
       1 AS dummy_feature
FROM {{ ref('g_included_author') }}