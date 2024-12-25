SELECT DISTINCT article_id,
                research_area_code
FROM {{ source('analitik', 'article_research_area') }}
WHERE research_area_rank = 1