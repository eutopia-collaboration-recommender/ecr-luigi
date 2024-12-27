SELECT author_id
FROM {{ ref('int_collaboration') }}
GROUP BY author_id
HAVING COUNT(DISTINCT article_id) >= 5