WITH src_crossref_publication AS (SELECT *
                                  FROM {{ source('lojze', 'crossref_publication') }})
SELECT p.publication_doi             AS article_doi,
       r.value ->> 'key'             AS reference_article_key,
       r.value ->> 'DOI'             AS reference_article_doi,
       r.value ->> 'ISSN'            AS reference_article_issn,
       r.value ->> 'ISBN'            AS reference_article_isbn,
       r.value ->> 'issue'           AS reference_article_issue,
       r.value ->> 'series-title'    AS reference_article_series_title,
       r.value ->> 'doi-asserted-by' AS reference_article_doi_asserted_by,
       r.value ->> 'first-page'      AS reference_article_first_page,
       r.value ->> 'component'       AS reference_article_component,
       r.value ->> 'article-title'   AS reference_article_title,
       r.value ->> 'volume-title'    AS reference_article_volume_title,
       r.value ->> 'volume'          AS reference_article_volume,
       r.value ->> 'author'          AS reference_article_author,
       r.value ->> 'year'            AS reference_article_year,
       r.value ->> 'unstructured'    AS reference_article_unstructured,
       r.value ->> 'edition'         AS reference_article_edition,
       r.value ->> 'journal-title'   AS reference_article_journal_title
FROM src_crossref_publication AS p,
     LATERAL JSONB_ARRAY_ELEMENTS(p.publication_metadata -> 'message' -> 'reference') AS r