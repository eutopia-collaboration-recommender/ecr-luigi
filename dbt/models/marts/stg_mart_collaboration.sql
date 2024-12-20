WITH ref_stg_mart_els_core_collaboration AS (SELECT article_id,
                                                    author_id,
                                                    institution_id
                                             FROM {{ ref('stg_mart_els_core_collaboration') }}),
     ref_stg_mart_els_enh_collaboration AS (SELECT article_id,
                                                   author_id,
                                                   institution_id
                                            FROM {{ ref('stg_mart_els_enh_collaboration') }}),
     ref_stg_mart_orcid_collaboration AS (SELECT article_id,
                                                 author_id,
                                                 institution_id
                                          FROM {{ ref('stg_mart_orcid_collaboration') }})

SELECT *
FROM ref_stg_mart_els_core_collaboration
UNION ALL
SELECT *
FROM ref_stg_mart_els_enh_collaboration
UNION ALL
SELECT *
FROM ref_stg_mart_orcid_collaboration