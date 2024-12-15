WITH src_crossref_top_n_research_area_publication as (SELECT publication_doi,
                                                             publication_metadata,
                                                             cerif_research_area_code,
                                                             publication_metadata -> 'indexed'   AS indexed_dt_json,
                                                             publication_metadata -> 'published' AS published_dt_json
                                                      FROM {{ source('lojze', 'crossref_top_n_research_area_publication')}}),
     publication_references AS (SELECT publication_doi,
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
                                FROM src_crossref_top_n_research_area_publication,
                                     LATERAL JSONB_ARRAY_ELEMENTS(publication_metadata -> 'reference') AS r),
     publication_references_agg
         AS (SELECT publication_doi,
                    STRING_AGG(
                            'Key: ' || COALESCE(reference_article_key, 'n/a') ||
                            ', DOI: ' || COALESCE(reference_article_doi, 'n/a') ||
                            ', ISSN: ' || COALESCE(reference_article_issn, 'n/a') ||
                            ', ISBN: ' || COALESCE(reference_article_isbn, 'n/a') ||
                            ', Issue: ' || COALESCE(reference_article_issue, 'n/a') ||
                            ', Series Title: ' || COALESCE(reference_article_series_title, 'n/a') ||
                            ', DOI Asserted By: ' || COALESCE(reference_article_doi_asserted_by, 'n/a') ||
                            ', First Page: ' || COALESCE(reference_article_first_page, 'n/a') ||
                            ', Component: ' || COALESCE(reference_article_component, 'n/a') ||
                            ', Article Title: ' || COALESCE(reference_article_title, 'n/a') ||
                            ', Volume Title: ' || COALESCE(reference_article_volume_title, 'n/a') ||
                            ', Volume: ' || COALESCE(reference_article_volume, 'n/a') ||
                            ', Author: ' || COALESCE(reference_article_author, 'n/a') ||
                            ', Year: ' || COALESCE(reference_article_year, 'n/a') ||
                            ', Unstructured: ' || COALESCE(reference_article_unstructured, 'n/a') ||
                            ', Edition: ' || COALESCE(reference_article_edition, 'n/a') ||
                            ', Journal Title: ' || COALESCE(reference_article_journal_title, 'n/a')
                        , '\n') AS references
             FROM publication_references
             GROUP BY publication_doi),
     publication_parsed AS (SELECT LOWER(c.publication_doi)                                                     AS article_doi
                                 , c.publication_metadata ->> 'URL'                                             AS article_url
                                 , c.publication_metadata ->> 'publisher'                                       AS article_publisher
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'title')                 AS article_title
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'short-title')           AS article_short_title
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'subtitle')              AS article_subtitle
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'original-title')        AS article_original_title
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'container-title')       AS article_container_title
                                 , JSONB_ARRAY_ELEMENTS_TEXT(c.publication_metadata -> 'short-container-title') AS article_short_container_title
                                 , c.publication_metadata ->> 'abstract'                                        AS article_abstract
                                 , c.publication_metadata -> 'reference'                                        AS article_reference
                                 , CAST(c.publication_metadata ->> 'is-referenced-by-count' AS INT)             AS is_referenced_by_count
                                 , r.references                                                                 AS article_references
                                 , {{ to_date_from_crossref_json('indexed_dt_json') }}                          AS article_indexed_dt
                                 , COALESCE({{ to_date_from_crossref_json('published_dt_json') }},
                                            {{ to_date_from_crossref_json('indexed_dt_json') }})                AS article_publication_dt
                            FROM src_crossref_top_n_research_area_publication c
                                     LEFT JOIN publication_references_agg r
                                               ON c.publication_doi = r.publication_doi)
SELECT article_doi,
       article_url,
       article_publisher,
       article_title,
       article_short_title,
       article_subtitle,
       article_original_title,
       article_container_title,
       article_short_container_title,
       article_abstract,
       is_referenced_by_count,
       article_references,
       article_indexed_dt,
       article_publication_dt,
       -- Embedding input for the article
       CONCAT_WS('\n', 'Title: ' || COALESCE(article_title, 'n/a'),
                 'Short Title: ' || COALESCE(article_short_title, 'n/a'),
                 'Subtitle: ' || COALESCE(article_subtitle, 'n/a'),
                 'Original Title: ' || COALESCE(article_original_title, 'n/a'),
                 'Container Title: ' || COALESCE(article_container_title, 'n/a'),
                 'Short Container Title: ' || COALESCE(article_short_container_title, 'n/a'),
                 'Abstract: ' || COALESCE(article_abstract, 'n/a'),
                 'References: ' || COALESCE(article_references, 'n/a')) AS article_embedding_input
FROM publication_parsed