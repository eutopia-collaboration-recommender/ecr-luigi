SELECT research_branch_code,
       research_branch_name,
       research_subbranch_code,
       research_subbranch_name,
       research_area_code,
       research_area_name
FROM {{ source('lojze', 'cerif_research_area') }}