WITH ref_stg_mart_els_collaboration AS (SELECT article_id,
                                               author_id,
                                               institution_id
                                        FROM {{ ref("stg_mart_els_collaboration") }}),
     ref_stg_mart_orcid_collaboration AS (SELECT article_id,
                                                 author_id,
                                                 institution_id
                                          FROM {{ ref('stg_mart_orcid_collaboration') }})
SELECT *
FROM ref_stg_mart_els_collaboration
UNION ALL
SELECT *
FROM ref_stg_mart_orcid_collaboration