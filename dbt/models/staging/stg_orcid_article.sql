WITH ref_stg_orcid_member_work AS (SELECT *
                                   FROM {{ source('lojze', 'orcid_member_works_parsed') }})
SELECT DISTINCT article_id,
                article_doi,
                article_eid,
                article_title,
                article_journal_title,
                article_publication_dt
FROM ref_stg_orcid_member_work