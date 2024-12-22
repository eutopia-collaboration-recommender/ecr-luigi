WITH ref_stg_mart_els_author AS (SELECT author_id,
                                        author_name
                                 FROM {{ ref('stg_mart_els_author') }}),
     ref_stg_mart_orcid_author AS (SELECT author_id,
                                          author_name
                                   FROM {{ ref('stg_mart_orcid_author') }}),
     merged AS (SELECT *
                FROM ref_stg_mart_els_author
                UNION ALL
                SELECT *
                FROM ref_stg_mart_orcid_author),
     filtered_authors AS (SELECT DISTINCT author_id
                          FROM {{ ref('stg_mart_collaboration') }})
SELECT DISTINCT m.author_id,
                FIRST_VALUE(m.author_name) OVER (PARTITION BY m.author_id
                    ORDER BY CASE WHEN m.author_name IS NOT NULL THEN 1 ELSE 0 END DESC, m.author_name DESC) AS author_name
FROM merged m
         INNER JOIN filtered_authors fa USING (author_id)