WITH ref_stg_orcid_member_work AS (SELECT *
                                   FROM {{ ref('stg_orcid_member_work') }})
SELECT DISTINCT article_id,
                article_title,
                article_journal_title,
                article_publication_dt
FROM ref_stg_orcid_member_work