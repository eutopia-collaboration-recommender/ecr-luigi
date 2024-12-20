CREATE OR REPLACE FUNCTION udf_to_date_from_crossref_json(
    json_path JSONB
) RETURNS DATE AS
$$
DECLARE
    dt DATE;
BEGIN
    -- Extract and cast the value as TIMESTAMP
    dt :=
            CASE
                WHEN json_path -> 'timestamp' IS NOT NULL THEN
                    TO_TIMESTAMP((json_path ->> 'timestamp')::BIGINT / 1000)
                WHEN json_path -> 'date-time' IS NOT NULL THEN
                    TO_DATE(json_path ->> 'date-time', 'YYYY-MM-DD')
                WHEN json_path -> 'date-parts' IS NOT NULL THEN
                    MAKE_DATE(
                            (json_path -> 'date-parts' -> 0 ->> 0)::INT,
                            COALESCE((json_path -> 'date-parts' -> 0 ->> 1)::INT, 1),
                            COALESCE((json_path -> 'date-parts' -> 0 ->> 2)::INT, 1)
                    )
                END;

    -- Return the extracted date
    RETURN dt;
END;
$$ LANGUAGE plpgsql;