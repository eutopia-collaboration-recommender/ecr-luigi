WITH ref_stg_orcid_member_person AS (SELECT member_id,
                                            member_scopus_id,
                                            member_family_name,
                                            member_given_names,
                                            member_loop_id,
                                            member_researcher_id
                                     FROM {{ ref('stg_orcid_member_person') }})
SELECT COALESCE(member_scopus_id, member_id) AS author_id,
       member_given_names || ' ' ||
       member_family_name                    AS author_name
FROM ref_stg_orcid_member_person