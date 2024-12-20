SELECT institution_id,
       institution_name,
       institution_pretty_name,
       institution_country,
       scopus_affiliation_id,
       institution_language,
       institution_country_iso2
FROM {{ source('lojze', 'eutopia_institution') }}