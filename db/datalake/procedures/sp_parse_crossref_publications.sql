SET SEARCH_PATH TO jezero;

CREATE OR REPLACE PROCEDURE sp_parse_crossref_publications(date_start DATE, date_end DATE)
    LANGUAGE plpgsql
AS
$$
BEGIN
    /*
    Delete existing records in crossref_publication_parsed table for given timeframe.
    */
    DELETE
    FROM crossref_publication_parsed
    WHERE article_publication_dt BETWEEN date_start AND date_end;

/*Table: temp_crossref_publication_filtered_dois
  Select DOIs to process (published date or indexed date is within the given timeframe).
*/
    DROP TABLE IF EXISTS temp_crossref_publication_filtered_dois;
    CREATE TEMP TABLE temp_crossref_publication_filtered_dois AS
    SELECT publication_doi
    FROM crossref_publication
    WHERE udf_to_date_from_crossref_json(publication_metadata -> 'message' -> 'published') BETWEEN '2014-01-01' AND '2014-12-31'
       OR (udf_to_date_from_crossref_json(publication_metadata -> 'message' -> 'published') IS NULL
        AND
           udf_to_date_from_crossref_json(publication_metadata -> 'message' -> 'indexed') BETWEEN '2014-01-01' AND '2014-12-31');


/*Table: temp_crossref_publication
  Select and parse publication metadata for the selected DOIs. Only article data, not authors and affiliations.
*/
    DROP TABLE IF EXISTS temp_crossref_publication;
    CREATE TEMP TABLE temp_crossref_publication AS
    SELECT c.publication_doi                                                                         AS article_doi,
           c.publication_metadata -> 'message' ->> 'URL'                                             AS article_url,
           c.publication_metadata -> 'message' ->> 'institution'                                     AS article_institution,
           c.publication_metadata -> 'message' ->> 'publisher'                                       AS article_publisher,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'title')                 AS article_title,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'short-title')           AS article_short_title,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'subtitle')              AS article_subtitle,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'original-title')        AS article_original_title,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'container-title')       AS article_container_title,
           JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'message' -> 'short-container-title') AS article_short_container_title,
           c.publication_metadata -> 'message' ->> 'abstract'                                        AS article_abstract,
           (c.publication_metadata -> 'message' ->> 'is-referenced-by-count')::INT                   AS article_referenced_by_count,
           c.publication_metadata -> 'message' -> 'author'                                           AS article_authors,
           udf_to_date_from_crossref_json(c.publication_metadata -> 'message' -> 'indexed')          AS indexed_dt,
           udf_to_date_from_crossref_json(c.publication_metadata -> 'message' -> 'published')        AS published_dt
    FROM crossref_publication c
             INNER JOIN temp_crossref_publication_filtered_dois f
                        ON f.publication_doi = c.publication_doi;

/*Table: temp_crossref_publication_authors
    Parse authors for the selected DOIs.
 */
    DROP TABLE IF EXISTS temp_crossref_publication_authors;
    CREATE TEMP TABLE temp_crossref_publication_authors AS
    SELECT c.article_doi,
           c.article_url,
           c.article_institution,
           c.article_publisher,
           c.article_title,
           c.article_short_title,
           c.article_subtitle,
           c.article_original_title,
           c.article_container_title,
           c.article_short_container_title,
           c.article_abstract,
           c.article_referenced_by_count,
           a.value ->> 'given'      AS author_given_name,
           a.value ->> 'family'     AS author_family_name,
           a.value ->> 'sequence'   AS author_sequence,
           a.value ->> 'ORCID'      AS author_orcid,
           a.value -> 'affiliation' AS author_affiliation,
           c.indexed_dt,
           c.published_dt
    FROM temp_crossref_publication c
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(c.article_authors) AS a ON TRUE;


/*Table: temp_crossref_publication_parsed
    Parse affiliations for the selected DOIs. This represents the final table with all the parsed data.
*/
    DROP TABLE IF EXISTS temp_crossref_publication_parsed;
    CREATE TEMP TABLE temp_crossref_publication_parsed AS
    SELECT DISTINCT c.article_doi,
                    c.article_url,
                    c.article_institution,
                    c.article_publisher,
                    c.article_title,
                    c.article_short_title,
                    c.article_subtitle,
                    c.article_original_title,
                    c.article_container_title,
                    c.article_short_container_title,
                    c.article_abstract,
                    c.indexed_dt                                     AS article_indexed_dt,
                    c.published_dt                                   AS article_publication_dt,
                    c.article_referenced_by_count,
                    c.author_given_name,
                    c.author_family_name,
                    c.author_sequence,
                    REPLACE(c.author_orcid, 'http://orcid.org/', '') AS author_orcid,
                    a.value ->> 'name'                               AS affiliation_identifier
    FROM temp_crossref_publication_authors c
             LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(c.author_affiliation) AS a ON TRUE;

    INSERT INTO crossref_publication_parsed
    (article_doi,
     article_url,
     article_institution,
     article_publisher,
     article_title,
     article_short_title,
     article_subtitle,
     article_original_title,
     article_container_title,
     article_short_container_title,
     article_abstract,
     article_indexed_dt,
     article_publication_dt,
     article_referenced_by_count,
     author_given_name,
     author_family_name,
     author_sequence,
     author_orcid,
     affiliation_identifier)
    SELECT article_doi,
           article_url,
           article_institution,
           article_publisher,
           article_title,
           article_short_title,
           article_subtitle,
           article_original_title,
           article_container_title,
           article_short_container_title,
           article_abstract,
           article_indexed_dt,
           article_publication_dt,
           article_referenced_by_count,
           author_given_name,
           author_family_name,
           author_sequence,
           author_orcid,
           affiliation_identifier
    FROM temp_crossref_publication_parsed;
END;
$$;

DO
$do$
    BEGIN
        FOR i in 2000..2024
            LOOP
                CALL sp_parse_crossref_publications(CONCAT(i, '-01-01')::DATE, CONCAT(i, '-12-31')::DATE);
                RAISE NOTICE 'Parsed Crossref publications for year: %', i;
            END LOOP;
    END;
$do$;
