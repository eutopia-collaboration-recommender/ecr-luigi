WITH ref_stg_orcid_member_employment AS (SELECT member_id,
                                                employment_id,
                                                affiliation_identifier,
                                                start_dt,
                                                end_dt
                                         FROM {{ ref('stg_orcid_member_employment') }}),
     src_orcid_member_work AS (SELECT member_id,
                                          article_id,
                                          article_publication_dt
                                   FROM {{ source('lojze', 'orcid_member_works_parsed') }})
SELECT e.member_id,
       e.employment_id,
       e.affiliation_identifier,
       w.article_id
FROM ref_stg_orcid_member_employment e
         LEFT JOIN src_orcid_member_work w
                   ON e.member_id = w.member_id
WHERE w.article_publication_dt >= e.start_dt
  AND w.article_publication_dt <= e.end_dt

