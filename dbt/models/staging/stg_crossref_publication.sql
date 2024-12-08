WITH unnested_publication as (SELECT c.publication_doi,
                                     c.publication_metadata -> 'message'              AS publication_metadata,
                                     a.author_json,
                                     a.author_index,
                                     af.affiliation_json,
                                     publication_metadata -> 'message' -> 'indexed'   AS indexed_dt_json,
                                     publication_metadata -> 'message' -> 'posted'    AS posted_dt_json,
                                     publication_metadata -> 'message' -> 'accepted'  AS accepted_dt_json,
                                     publication_metadata -> 'message' -> 'submitted' AS submitted_dt_json,
                                     publication_metadata -> 'message' -> 'reviewed'  AS reviewed_dt_json,
                                     publication_metadata -> 'message' -> 'assertion' AS assertion_dt_json,
                                     publication_metadata -> 'message' -> 'published' AS published_dt_json
                              FROM {{ source('lojze', 'crossref_publication')}} c,
                                   LATERAL JSONB_ARRAY_ELEMENTS(c.publication_metadata #> '{message,author}')
                                       WITH ORDINALITY AS a (author_json, author_index),
                                   LATERAL JSONB_ARRAY_ELEMENTS(a.author_json -> 'affiliation') AS af (affiliation_json))
SELECT LOWER(publication_doi)                                                                AS article_doi
     , author_index
     , author_json ->> 'sequence'                                                            AS author_sequence
     , CONCAT(
        COALESCE(author_json ->> 'given' || ' ', ''),
        COALESCE(author_json ->> 'family', '')
       )                                                                                     AS author_full_name
     -- Author ORCID
     , (REGEXP_MATCHES(author_json ->> 'ORCID', '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4})'))[1] AS author_orcid
     , COALESCE(CAST(author_json ->> 'authenticated-orcid' AS BOOLEAN),
                FALSE)                                                                       AS is_author_orcid_authenticated
     , affiliation_json ->> 'name'                                                           AS affiliation_identifier
     , publication_metadata ->> 'URL'                                                        AS article_url
     , publication_metadata ->> 'funder'                                                     AS article_funder
     , publication_metadata ->> 'institution'                                                AS article_institution
     , publication_metadata ->> 'publisher'                                                  AS article_publisher
     , publication_metadata ->> 'title'                                                      AS article_title
     , publication_metadata ->> 'short-title'                                                AS article_short_title
     , publication_metadata ->> 'subtitle'                                                   AS article_subtitle
     , publication_metadata ->> 'original-title'                                             AS article_original_title
     , publication_metadata ->> 'container-title'                                            AS article_container_title
     , publication_metadata ->> 'short-container-title'                                      AS article_short_container_title
     , publication_metadata ->> 'abstract'                                                   AS article_abstract
     , publication_metadata ->> 'reference'                                                  AS article_reference
     , CAST(publication_metadata ->> 'is-referenced-by-count' AS INT)                        AS is_referenced_by_count
     , {{ to_date_from_crossref_json('indexed_dt_json') }}                                   AS article_indexed_dt
     , LEAST(
        {{ to_date_from_crossref_json('posted_dt_json') }},
        {{ to_date_from_crossref_json('accepted_dt_json') }},
        {{ to_date_from_crossref_json('submitted_dt_json') }},
        {{ to_date_from_crossref_json('reviewed_dt_json') }},
        {{ to_date_from_crossref_json('assertion_dt_json') }},
        {{ to_date_from_crossref_json('published_dt_json') }},
        {{ to_date_from_crossref_json('indexed_dt_json') }}
       )                                                                                     AS article_publication_dt
FROM unnested_publication