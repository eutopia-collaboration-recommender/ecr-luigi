CREATE OR REPLACE FUNCTION udf_is_eutopia_institution(affiliation_identifier TEXT)
    RETURNS TEXT AS
$$
DECLARE
    affiliation TEXT;
BEGIN
    -- Safely construct a valid date by using the `MAKE_DATE` function, handling invalid days and months
    BEGIN
        -- Attempt to construct the date
        affiliation := CASE
                           WHEN LOWER(affiliation_identifier) LIKE '%ljubljan%' THEN
                               'UNI_LJ'
                           WHEN LOWER(affiliation_identifier) LIKE '%brussel%' THEN
                               'VUB'
                           WHEN LOWER(affiliation_identifier) LIKE '%dresden%' THEN
                               'TU_DRESDEN'
                           WHEN LOWER(affiliation_identifier) LIKE '%gothenburg%' OR
                                LOWER(affiliation_identifier) LIKE '%göteborgs%' THEN
                               'GU'
                           WHEN LOWER(affiliation_identifier) LIKE '%cergy%' OR
                                LOWER(affiliation_identifier) LIKE '%paris%' THEN
                               'CY'
                           WHEN LOWER(affiliation_identifier) LIKE '%lisbon%' OR
                                LOWER(affiliation_identifier) LIKE '%nova%' THEN
                               'UNL'
                           WHEN LOWER(affiliation_identifier) LIKE '%warwick%' THEN
                               'WARWICK'
                           WHEN LOWER(affiliation_identifier) LIKE '%barcelona%' OR
                                LOWER(affiliation_identifier) LIKE '%pompeu%fabra%' THEN
                               'UPF'
                           WHEN LOWER(affiliation_identifier) LIKE '%venice%' OR
                                LOWER(affiliation_identifier) LIKE '%foscari%' THEN
                               'UNIVE'
                           WHEN LOWER(affiliation_identifier) LIKE '%babes%'
                               OR LOWER(affiliation_identifier) LIKE '%bolyai%' THEN
                               'UBBCLUJ'
                           WHEN affiliation_identifier IS NULL THEN
                               'n/a'
                           ELSE
                               'OTHER'
            END;
    END;

    -- Return the valid date
    RETURN affiliation;
END;
$$ LANGUAGE plpgsql;