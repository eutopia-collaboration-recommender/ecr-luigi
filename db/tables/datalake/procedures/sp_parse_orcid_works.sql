CREATE OR REPLACE PROCEDURE sp_parse_orcid_member_works(input_year INT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    DELETE
    FROM orcid_member_works_parsed
    WHERE DATE_PART('year', article_publication_dt) = input_year;
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
           ws.value AS member_work
    FROM temp_orcid_member_works mw
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(mw.member_works) AS w ON TRUE
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(w -> 'work-summary') AS ws ON TRUE
    WHERE ws.value #>> '{publication-date,year,value}' = input_year::TEXT;

    DROP TABLE IF EXISTS temp_orcid_member_works_flattened;
    CREATE TEMP TABLE temp_orcid_member_works_flattened AS
    SELECT DISTINCT mw.member_id,
                    mw.member_work #>> '{put-code}'                 AS article_id,
                    mw.member_work #>> '{url,value}'                AS article_url,
                    eid.value #>> '{external-id-url,value}'         AS article_external_id_url,
                    eid.value #>> '{external-id-type}'              AS article_external_id_type,
                    eid.value #>> '{external-id-value}'             AS article_external_id,
                    eid.value #>> '{external-id-relationship}'      AS article_external_id_relationship,
                    mw.member_work #>> '{title,title,value}'        AS article_title,
                    COALESCE((mw.member_work #>> '{publication-date,year,value}')::INT,
                             1900)                                  AS article_publication_year,
                    COALESCE((mw.member_work #>> '{publication-date,month,value}')::INT,
                             1)                                     AS article_publication_month,
                    COALESCE((mw.member_work #>> '{publication-date,day,value}')::INT,
                             1)                                     AS article_publication_day,
                    mw.member_work #>> '{type}'                     AS article_type,
                    mw.member_work #>> '{last-modified-date,value}' AS article_last_modified_dt,
                    mw.member_work #>> '{journal-title,value}'      AS article_journal_title
    FROM temp_orcid_member_works_filtered mw
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(mw.member_work -> 'external-ids' -> 'external-id') AS eid ON TRUE;

    DROP TABLE IF EXISTS temp_orcid_member_works_by_doi;
    CREATE TEMP TABLE temp_orcid_member_works_by_doi AS
    SELECT DISTINCT article_id,
                    article_external_id,
                    article_url like '%' || article_external_id || '%'                 as is_primary_external_id,
                    rank() over (partition by article_id order by case
                                                                      when article_url like '%' || article_external_id || '%'
                                                                          then 1
                                                                      else 0 end desc) as rn
    FROM temp_orcid_member_works_flattened
    WHERE article_external_id_type = 'doi'
      AND article_external_id_relationship = 'self';


    DROP TABLE IF EXISTS temp_orcid_member_works_by_eid;
    CREATE TEMP TABLE temp_orcid_member_works_by_eid AS
    SELECT DISTINCT article_id,
                    article_external_id,
                    article_url like '%' || article_external_id || '%'                 as is_primary_external_id,
                    rank() over (partition by article_id order by case
                                                                      when article_url like '%' || article_external_id || '%'
                                                                          then 1
                                                                      else 0 end desc) as rn
    FROM temp_orcid_member_works_flattened
    WHERE article_external_id_type = 'eid'
      AND article_external_id_relationship = 'self';


    DROP TABLE IF EXISTS temp_orcid_member_works_with_dois;
    CREATE TEMP TABLE temp_orcid_member_works_with_dois AS
    SELECT DISTINCT m.member_id,
                    m.article_id,
                    d.article_external_id                                              AS article_doi,
                    m.article_title,
                    m.article_type,
                    m.article_journal_title,
                    MAX(TO_TIMESTAMP(m.article_last_modified_dt::BIGINT / 1000)::DATE) AS article_last_modified_dt,
                    MAX(UDF_MAKE_DATE_SAFE(m.article_publication_year, m.article_publication_month,
                                           m.article_publication_day))                 AS article_publication_dt
    FROM temp_orcid_member_works_flattened m
             LEFT JOIN temp_orcid_member_works_by_doi d
                       ON m.article_id = d.article_id
                           AND d.rn = 1
    GROUP BY m.member_id, m.article_id, d.article_external_id, m.article_title, m.article_type, m.article_journal_title;

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
             LEFT JOIN temp_orcid_member_works_by_eid e
                       ON m.article_id = e.article_id
                           AND e.rn = 1;


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