WITH src_orcid_member_person AS (SELECT member_id,
                                       member_person
                                FROM {{ source('lojze', 'orcid_member_person') }}),
    external_id_by_author as (SELECT member_id,
                                      member_person,
                                      external_identifiers #>> '{external-id-type}'  AS external_identifier_type,
                                      external_identifiers #>> '{external-id-value}' AS external_identifier_value
                               FROM src_orcid_member_person
                                    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(member_person #> '{external-identifiers,external-identifier}') AS external_identifiers ON TRUE)
SELECT main.member_id,
       main.member_person #>> '{name,family-name,value}' AS member_family_name,
       main.member_person #>> '{name,given-names,value}' AS member_given_names,
       scopus_id.external_identifier_value               AS member_scopus_id,
       researcher_id.external_identifier_value           AS member_researcher_id,
       loop_id.external_identifier_value                 AS member_loop_id
FROM src_orcid_member_person AS main
         LEFT JOIN external_id_by_author AS scopus_id
                   ON main.member_id = scopus_id.member_id
                       AND scopus_id.external_identifier_type = 'Scopus Author ID'
         LEFT JOIN external_id_by_author AS researcher_id
                   ON main.member_id = researcher_id.member_id
                       AND researcher_id.external_identifier_type = 'ResearcherID'
         LEFT JOIN external_id_by_author AS loop_id
                   ON main.member_id = loop_id.member_id
                       AND loop_id.external_identifier_type = 'Loop profile'
