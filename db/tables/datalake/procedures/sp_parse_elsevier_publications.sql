SET SEARCH_PATH TO jezero;

CREATE OR REPLACE PROCEDURE sp_parse_elsevier_publications(date_start DATE, date_end DATE)
    LANGUAGE plpgsql
AS
$$
BEGIN

    DELETE
    FROM elsevier_publication_parsed
    WHERE article_publication_dt BETWEEN date_start AND date_end;

    /* TABLE: temp_elsevier_publication
    Parse Elsevier publications that were published between date_start and date_end.
    This part does not yet parse keywords, authors, and affiliations.
    */
    DROP TABLE IF EXISTS temp_elsevier_publication;
    CREATE TEMP TABLE temp_elsevier_publication AS
    SELECT e.publication_id             AS article_id,
           e.publication_eid            AS article_eid,
           e.publication_doi            AS article_doi,
           e.publication_title          AS article_title,
           e.publication_type           AS article_type,
           e.publication_abstract       AS article_abstract,
           e.publication_citation_count AS article_citation_count,
           e.publication_dt             AS article_publication_dt,
           e.publication_references     AS article_references,
           e.publication_authors        AS article_authors
    FROM elsevier_publication e
    WHERE e.publication_dt BETWEEN date_start AND date_end;
    /* TABLE: temp_elsevier_publication_authors
    Parse Elsevier publication authors and store their affiliations.
     */
    DROP TABLE IF EXISTS temp_elsevier_publication_authors;
    CREATE TEMP TABLE temp_elsevier_publication_authors AS
    SELECT e.article_id,
           e.article_eid,
           e.article_doi,
           e.article_title,
           e.article_type,
           e.article_abstract,
           e.article_citation_count,
           e.article_publication_dt,
           e.article_references,
           a.value ->> 'author_id'             AS author_id,
           a.value ->> 'author_initials'       AS author_initials,
           a.value ->> 'author_last_name'      AS author_last_name,
           a.value ->> 'author_first_name'     AS author_first_name,
           a.value ->> 'author_indexed_name'   AS author_indexed_name,
           a.value -> 'author_affiliation_ids' AS author_affiliations
    FROM temp_elsevier_publication e
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(e.article_authors) AS a ON TRUE;
    /* TABLE: temp_elsevier_publication_affiliations
    Parse Elsevier publication affiliations.
     */
    DROP TABLE IF EXISTS temp_elsevier_publication_affiliations;
    CREATE TEMP TABLE temp_elsevier_publication_affiliations AS
    SELECT e.article_id,
           e.article_eid,
           e.article_doi,
           e.article_title,
           e.article_type,
           e.article_abstract,
           e.article_citation_count,
           e.article_publication_dt,
           e.article_references,
           e.author_id,
           e.author_initials,
           e.author_last_name,
           e.author_first_name,
           e.author_indexed_name,
           af.value ->> 'id' AS affiliation_id
    FROM temp_elsevier_publication_authors e
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(e.author_affiliations) AS af ON TRUE;


    /* TABLE: temp_elsevier_publication_keywords
    Parse Elsevier publication keywords for publications that were published between date_start and date_end.
      */
    DROP TABLE IF EXISTS temp_elsevier_publication_keywords;
    CREATE TEMP TABLE temp_elsevier_publication_keywords AS
    SELECT e.publication_id,
           STRING_AGG(k ->> 'keyword', ', ') AS publication_keywords
    FROM elsevier_publication e,
         LATERAL JSONB_ARRAY_ELEMENTS(e.publication_keywords) AS k
    WHERE e.publication_dt BETWEEN date_start AND date_end
    GROUP BY e.publication_id;

    /* TABLE: temp_elsevier_publication_parsed
       Join all parsed Elsevier publication data together.
     */
    DROP TABLE IF EXISTS temp_elsevier_publication_parsed;
    CREATE TEMP TABLE temp_elsevier_publication_parsed AS
    SELECT e.article_id,
           e.article_eid,
           e.article_doi,
           e.article_title,
           e.article_type,
           e.article_abstract,
           e.article_citation_count,
           e.article_publication_dt,
           e.article_references,
           e.author_id,
           e.author_initials,
           e.author_last_name,
           e.author_first_name,
           e.author_indexed_name,
           e.affiliation_id,
           k.publication_keywords AS article_keywords
    FROM temp_elsevier_publication_affiliations e
             LEFT JOIN temp_elsevier_publication_keywords k
                       ON e.article_id = k.publication_id;


    INSERT INTO elsevier_publication_parsed (article_id,
                                             article_eid,
                                             article_doi,
                                             article_title,
                                             article_type,
                                             article_abstract,
                                             article_citation_count,
                                             article_publication_dt,
                                             author_id,
                                             author_initials,
                                             author_last_name,
                                             author_first_name,
                                             author_indexed_name,
                                             affiliation_id,
                                             article_keywords)
    SELECT article_id,
           article_eid,
           article_doi,
           article_title,
           article_type,
           article_abstract,
           article_citation_count,
           article_publication_dt,
           author_id,
           author_initials,
           author_last_name,
           author_first_name,
           author_indexed_name,
           affiliation_id,
           article_keywords
    FROM temp_elsevier_publication_parsed;
END;
$$;

CALL sp_parse_elsevier_publications('2020-01-01', '2020-12-31');

DO
$do$
    BEGIN
        FOR i in 2021..2024
            LOOP
                CALL sp_parse_elsevier_publications(CONCAT(i, '-01-01')::DATE, CONCAT(i, '-12-31')::DATE);
                RAISE NOTICE 'Parsed Elsevier publications for year: %', i;
            END LOOP;
    END;
$do$;
