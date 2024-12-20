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
                FROM ref_stg_mart_orcid_author)
SELECT DISTINCT author_id,
                FIRST_VALUE(author_name) OVER (PARTITION BY author_id
                    ORDER BY CASE WHEN author_name IS NOT NULL THEN 1 ELSE 0 END DESC, author_name DESC) AS author_name
FROM merged