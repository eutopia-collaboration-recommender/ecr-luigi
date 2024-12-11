WITH src_elsevier_publication AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication') }}),
     elsevier_publication_keywords AS (SELECT e.publication_id,
                                              STRING_AGG(k ->> 'keyword', ', ') AS publication_keywords
                                       FROM src_elsevier_publication e,
                                            LATERAL JSONB_ARRAY_ELEMENTS(e.publication_keywords) AS k
                                       GROUP BY e.publication_id)
SELECT e.publication_id                  AS article_id,
       e.publication_eid                 AS article_eid,
       e.publication_doi                 AS article_doi,
       e.publication_title               AS article_title,
       e.publication_type                AS article_type,
       e.publication_abstract            AS article_abstract,
       e.publication_citation_count      AS article_citation_count,
       e.publication_dt                  AS article_publication_dt,
       e.publication_references          AS article_references,
       k.publication_keywords            AS article_keywords,
       a.value ->> 'author_id'           AS author_id,
       a.value ->> 'author_initials'     AS author_initials,
       a.value ->> 'author_last_name'    AS author_last_name,
       a.value ->> 'author_first_name'   AS author_first_name,
       a.value ->> 'author_indexed_name' AS author_indexed_name,
       af.value ->> 'id'                 AS affiliation_id
FROM src_elsevier_publication e
         LEFT JOIN elsevier_publication_keywords k
                   ON e.publication_id = k.publication_id,
     LATERAL JSONB_ARRAY_ELEMENTS(e.publication_authors) AS a,
     LATERAL JSONB_ARRAY_ELEMENTS(a.value -> 'author_affiliation_ids') AS af