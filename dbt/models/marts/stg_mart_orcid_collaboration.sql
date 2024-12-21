WITH ref_stg_mart_orcid_collaboration_with_duplicates AS (SELECT DISTINCT article_id,
                                                                          author_id,
                                                                          institution_id
                                                          FROM {{ ref('stg_mart_orcid_collaboration_with_duplicates') }}),
     ref_stg_mart_orcid_article_with_duplicates AS (SELECT DISTINCT article_id,
                                                                    new_article_id
                                                    FROM {{ ref('stg_mart_orcid_article_with_duplicates') }})
SELECT DISTINCT COALESCE(a.new_article_id, c.article_id) AS article_id,
                c.author_id,
                c.institution_id
FROM ref_stg_mart_orcid_collaboration_with_duplicates c
         LEFT JOIN ref_stg_mart_orcid_article_with_duplicates a USING (article_id)