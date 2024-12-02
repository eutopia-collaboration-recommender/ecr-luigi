WITH src_datalake_orcid_member_works AS (SELECT member_id,
                                                member_works,
                                                row_created_at,
                                                row_updated_at
                                         FROM {{ source('lojze', 'orcid_member_works') }})

SELECT mw.member_id
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
     , mw.row_created_at
     , mw.row_updated_at
FROM src_datalake_orcid_member_works AS mw,
     LATERAL JSONB_ARRAY_ELEMENTS(mw.member_works) AS w,
     LATERAL JSONB_ARRAY_ELEMENTS(w -> 'work-summary') AS ws,
     LATERAL JSONB_ARRAY_ELEMENTS(w -> 'external-ids' -> 'external-id') AS eid
