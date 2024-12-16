CREATE OR REPLACE FUNCTION udf_make_date_safe(year INT, month INT, day INT)
RETURNS DATE AS
$$
DECLARE
    dt DATE;
BEGIN
    -- Safely construct a valid date by using the `MAKE_DATE` function, handling invalid days and months
    BEGIN
        -- Attempt to construct the date
        dt := MAKE_DATE(year,
                        COALESCE(month,
                        day);
    EXCEPTION WHEN OTHERS THEN
        -- If invalid, handle edge cases (e.g., November 31 becomes November 30)
        BEGIN
            -- Adjust the date to the last valid day of the given month
            dt := (TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD') + INTERVAL '1 month' - INTERVAL '1 day')::DATE;
            IF day > EXTRACT(DAY FROM dt) THEN
                -- Use the last valid day of the month
                RETURN dt;
            END IF;
        END;
    END;

    -- Return the valid date
    RETURN dt;
END;
$$ LANGUAGE plpgsql;