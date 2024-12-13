SELECT c.publication_doi,
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
     LATERAL JSONB_ARRAY_ELEMENTS(a.author_json -> 'affiliation') AS af (affiliation_json)