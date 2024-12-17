SET SEARCH_PATH TO jezero;

CREATE OR REPLACE PROCEDURE sp_parse_orcid_member_works(year INT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    DELETE
    FROM orcid_member_works_parsed
    WHERE DATE_PART('year', article_publication_dt) = 2011;

    DROP TABLE IF EXISTS temp_orcid_member_works_max_ts;
    CREATE TEMP TABLE temp_orcid_member_works_max_ts AS
    SELECT member_id,
           MAX(row_updated_at) AS row_updated_at
    FROM orcid_member_works
    GROUP BY member_id;

    DROP TABLE IF EXISTS temp_orcid_member_works;
    CREATE TEMP TABLE temp_orcid_member_works AS
    SELECT mw.member_id,
           mw.member_works
    FROM orcid_member_works mw
             INNER JOIN temp_orcid_member_works_max_ts ts
                        ON mw.member_id = ts.member_id
                            AND mw.row_updated_at = ts.row_updated_at;

    DROP TABLE IF EXISTS temp_orcid_member_works_filtered;
    CREATE TEMP TABLE temp_orcid_member_works_filtered AS
    SELECT mw.member_id,
           mw.member_works,
           w.value AS member_work
    FROM temp_orcid_member_works mw,
         LATERAL JSONB_ARRAY_ELEMENTS(mw.member_works) AS w,
         LATERAL JSONB_ARRAY_ELEMENTS(w -> 'work-summary') AS ws
    WHERE ws.value #>> '{publication-date,year,value}' = '2011';


    DROP TABLE IF EXISTS temp_orcid_member_works_flattened;
    CREATE TEMP TABLE temp_orcid_member_works_flattened AS
    SELECT DISTINCT mw.member_id,
                    ws.value #>> '{path}'                                         AS article_id,
                    eid.value #>> '{external-id-url,value}'                       AS article_external_id_url,
                    eid.value #>> '{external-id-type}'                            AS article_external_id_type,
                    eid.value #>> '{external-id-value}'                           AS article_external_id,
                    ws.value #>> '{title,title,value}'                            AS article_title,
                    COALESCE((ws #>> '{publication-date,year,value}')::INT, 1900) AS article_publication_year,
                    COALESCE((ws #>> '{publication-date,month,value}')::INT, 1)   AS article_publication_month,
                    COALESCE((ws #>> '{publication-date,day,value}')::INT, 1)     AS article_publication_day,
                    ws.value #>> '{type}'                                         AS article_type,
                    ws.value #>> '{last-modified-date,value}'                     AS article_last_modified_dt,
                    ws.value #>> '{journal-title,value}'                          AS article_journal_title
    FROM temp_orcid_member_works_filtered mw,
         LATERAL JSONB_ARRAY_ELEMENTS(mw.member_work -> 'work-summary') AS ws,
         LATERAL JSONB_ARRAY_ELEMENTS(mw.member_work -> 'external-ids' -> 'external-id') AS eid;

    DROP TABLE IF EXISTS temp_orcid_member_works_by_doi;
    CREATE TEMP TABLE temp_orcid_member_works_by_doi AS
    SELECT DISTINCT article_id,
                    article_external_id
    FROM temp_orcid_member_works_flattened
    WHERE article_external_id_type = 'doi';

    DROP TABLE IF EXISTS temp_orcid_member_works_by_eid;
    CREATE TEMP TABLE temp_orcid_member_works_by_eid AS
    SELECT DISTINCT article_id,
                    article_external_id
    FROM temp_orcid_member_works_flattened
    WHERE article_external_id_type = 'eid';

    DROP TABLE IF EXISTS temp_orcid_member_works_with_dois;
    CREATE TEMP TABLE temp_orcid_member_works_with_dois AS
    SELECT DISTINCT m.member_id,
                    m.article_id,
                    d.article_external_id                                         AS article_doi,
                    m.article_title,
                    udf_make_date_safe(m.article_publication_year, m.article_publication_month,
                                       m.article_publication_day)                 AS article_publication_dt,
                    m.article_type,
                    TO_TIMESTAMP(m.article_last_modified_dt::BIGINT / 1000)::DATE AS article_last_modified_dt,
                    m.article_journal_title
    FROM temp_orcid_member_works_flattened m
             LEFT JOIN temp_orcid_member_works_by_doi d USING (article_id);


    DROP TABLE IF EXISTS temp_orcid_member_works_parsed;
    CREATE TEMP TABLE temp_orcid_member_works_parsed AS
    SELECT DISTINCT m.member_id,
                    m.article_id,
                    m.article_doi,
                    e.article_external_id AS article_eid,
                    m.article_title,
                    m.article_publication_dt,
                    m.article_type,
                    m.article_last_modified_dt,
                    m.article_journal_title
    FROM temp_orcid_member_works_with_dois m
             LEFT JOIN temp_orcid_member_works_by_eid e USING (article_id);


    INSERT INTO orcid_member_works_parsed (member_id,
                                           article_id,
                                           article_doi,
                                           article_eid,
                                           article_title,
                                           article_publication_dt,
                                           article_type,
                                           article_last_modified_dt,
                                           article_journal_title)
    SELECT member_id,
           article_id,
           article_doi,
           article_eid,
           article_title,
           article_publication_dt,
           article_type,
           article_last_modified_dt,
           article_journal_title
    FROM temp_orcid_member_works_parsed;
END;
$$;

call sp_parse_orcid_member_works(2011);


DO
$do$
    BEGIN
        FOR i in 2000..2024
            LOOP
                CALL sp_parse_orcid_member_works(i);
                RAISE NOTICE 'Parsed ORCID publications for year: %', i;
            END LOOP;
    END;
$do$;
