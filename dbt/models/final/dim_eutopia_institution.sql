SELECT institution_id,
       institution_name,
       institution_pretty_name,
       institution_country,
       scopus_affiliation_id,
       institution_language,
       institution_country_iso2
FROM {{ source('lojze', 'eutopia_institution') }}
UNION ALL
SELECT 'OTHER' as institution_id,
       'Other' as institution_name,
       'n/a'   as institution_pretty_name,
       'n/a'   as institution_country,
       'n/a'   as scopus_affiliation_id,
       'n/a'   as institution_language,
       'n/a'   as institution_country_iso2