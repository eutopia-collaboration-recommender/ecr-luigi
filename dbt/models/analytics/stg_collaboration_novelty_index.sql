SELECT article_id,
       MAX(collaboration_novelty_index) AS collaboration_novelty_index
FROM {{ source('analitik', 'collaboration_novelty_index') }}
GROUP BY article_id