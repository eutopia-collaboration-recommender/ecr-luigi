WITH src_datalake_orcid_member_works AS (SELECT member_id,
                                                member_works,
                                                row_created_at,
                                                row_updated_at
                                         FROM {{ source('lojze', 'orcid_member_works') }})
   , member_work_by_external_id AS (SELECT mw.member_id
                                         , ws ->> 'put-code'                       AS article_id
                                         , eid #>> '{external-id-url,value}'       AS article_external_id_url
                                         , eid ->> 'external-id-type'              AS article_external_id_type
                                         , eid ->> 'external-id-value'             AS article_external_id
                                         , ws #>> '{title,title,value}'            AS article_title
                                         , ws #>> '{publication-date,year,value}'  AS article_publication_year
                                         , ws #>> '{publication-date,month,value}' AS article_publication_month
                                         , ws #>> '{publication-date,day,value}'   AS article_publication_day
                                         , ws ->> 'type'                           AS article_type
                                         , ws #>> '{last-modified-date,value}'     AS article_last_modified_dt
                                         , ws #>> '{journal-title,value}'          AS article_journal_title
                                    FROM src_datalake_orcid_member_works AS mw,
                                         LATERAL JSONB_ARRAY_ELEMENTS(mw.member_works) AS w,
                                         LATERAL JSONB_ARRAY_ELEMENTS(w -> 'work-summary') AS ws,
                                         LATERAL JSONB_ARRAY_ELEMENTS(w -> 'external-ids' -> 'external-id') AS eid)
   , member_work_doi AS (SELECT DISTINCT article_id,
                                         article_external_id
                         FROM member_work_by_external_id
                         WHERE article_external_id_type = 'doi')
   , member_work_eid AS (SELECT DISTINCT article_id,
                                         article_external_id
                         FROM member_work_by_external_id
                         WHERE article_external_id_type = 'eid')
SELECT DISTINCT m.member_id,
                m.article_id,
                d.article_external_id AS article_doi,
                e.article_external_id AS article_eid,
                m.article_title,
                MAKE_DATE(
                        m.article_publication_year::INT,
                        CASE
                            WHEN m.article_publication_month::INT = 0 OR m.article_publication_month IS NULL THEN 1
                            ELSE m.article_publication_month::INT END,
                        COALESCE(m.article_publication_day::INT, 1)
                )                     AS article_publication_dt,
                m.article_type,
                m.article_last_modified_dt,
                m.article_journal_title
FROM member_work_by_external_id m
         LEFT JOIN member_work_doi d USING (article_id)
         LEFT JOIN member_work_eid e USING (article_id)