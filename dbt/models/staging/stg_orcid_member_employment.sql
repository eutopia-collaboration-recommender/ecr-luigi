WITH src_datalake_orcid_member_employments AS (SELECT member_id,
                                                      member_employments,
                                                      row_created_at,
                                                      row_updated_at
                                               FROM {{ source('lojze', 'orcid_member_employments') }}),
     member_employment AS (SELECT m.member_id,
                                  es.value #>> '{employment-summary,put-code}'                     AS employment_id,
                                  es.value #>> '{employment-summary,organization,name}'            AS organization_name,
                                  es.value #>> '{employment-summary,organization,address,city}'    AS organization_city,
                                  es.value #>> '{employment-summary,organization,address,region}'  AS organization_region,
                                  es.value #>> '{employment-summary,organization,address,country}' AS organization_country,
                                  es.value #>> '{employment-summary,department-name}'              AS department_name,
                                  es.value #>> '{employment-summary,role-title}'                   AS role_title,
                                  COALESCE(
                                          MAKE_DATE((es.value #>> '{employment-summary,start-date,year,value}')::INT,
                                                    COALESCE(
                                                            (es.value #>> '{employment-summary,start-date,month,value}')::INT,
                                                            1),
                                                    COALESCE(
                                                            (es.value #>> '{employment-summary,start-date,day,value}')::INT,
                                                            1)),
                                          MAKE_DATE(1900, 1, 1))                                   AS start_dt,
                                  CASE
                                      -- This is a special case for the ORCID record, because we get 31st of November as the end date from the source
                                      WHEN m.member_id = '0000-0002-9449-2952' THEN MAKE_DATE(1996, 11, 30)
                                      ELSE
                                          COALESCE(
                                                  MAKE_DATE(
                                                          (es.value #>> '{employment-summary,end-date,year,value}')::INT,
                                                          COALESCE(
                                                                  (es.value #>> '{employment-summary,end-date,month,value}')::INT,
                                                                  1),
                                                          COALESCE(
                                                                  (es.value #>> '{employment-summary,end-date,day,value}')::INT,
                                                                  1)),
                                                  MAKE_DATE(2100, 1, 1))
                                      END                                                          AS end_dt
                           FROM src_datalake_orcid_member_employments AS m,
                                LATERAL JSONB_ARRAY_ELEMENTS(member_employments) AS e,
                                LATERAL JSONB_ARRAY_ELEMENTS(e -> 'summaries') AS es)
SELECT member_id,
       employment_id,
       CONCAT_WS('_',
                 organization_name,
                 organization_city,
                 organization_region,
                 organization_country,
                 department_name,
                 role_title) AS affiliation_identifier,
       organization_name,
       organization_city,
       organization_region,
       organization_country,
       department_name,
       role_title,
       start_dt,
       end_dt
FROM member_employment
