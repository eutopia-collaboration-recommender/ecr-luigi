{% macro get_eutopia_institution(affiliation_identifier) %}
( CASE
    WHEN LOWER (affiliation_identifier) LIKE '%ljubljan%' THEN
    'UNI_LJ'
    WHEN LOWER (affiliation_identifier) LIKE '%brussel%' THEN
    'VUB'
    WHEN LOWER (affiliation_identifier) LIKE '%dresden%' THEN
    'TU_DRESDEN'
    WHEN LOWER (affiliation_identifier) LIKE '%gothenburg%' OR
    LOWER (affiliation_identifier) LIKE '%göteborgs%' THEN
    'GU'
    WHEN LOWER (affiliation_identifier) LIKE '%cergy%' OR
    LOWER (affiliation_identifier) LIKE '%paris%' THEN
    'CY'
    WHEN LOWER (affiliation_identifier) LIKE '%lisbon%' OR
    LOWER (affiliation_identifier) LIKE '%nova%' THEN
    'UNL'
    WHEN LOWER (affiliation_identifier) LIKE '%warwick%' THEN
    'WARWICK'
    WHEN LOWER (affiliation_identifier) LIKE '%barcelona%' OR
    LOWER (affiliation_identifier) LIKE '%pompeu%fabra%' THEN
    'UPF'
    WHEN LOWER (affiliation_identifier) LIKE '%venice%' OR
    LOWER (affiliation_identifier) LIKE '%foscari%' THEN
    'UNIVE'
    WHEN LOWER (affiliation_identifier) LIKE '%babes%'
    OR LOWER (affiliation_identifier) LIKE '%bolyai%' THEN
    'UBBCLUJ'
    ELSE
    'OTHER'
    END)
{% endmacro %}