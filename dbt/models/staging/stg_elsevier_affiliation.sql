WITH src_elsevier_publication_affiliation AS (SELECT *
                                              FROM {{ source('lojze', 'elsevier_publication_affiliation') }})
   , ref_dim_eutopia_institution AS (SELECT institution_id,
                                            scopus_affiliation_id
                                     FROM {{ ref('dim_eutopia_institution') }})
   , elsevier_affiliation AS (SELECT REPLACE(e.publication_id, 'SCOPUS_ID:', '') AS article_id,
                                     e.publication_eid                           AS article_eid,
                                     e.publication_doi                           AS article_doi,
                                     i.institution_id
                              FROM src_elsevier_publication_affiliation e
                                       LEFT JOIN ref_dim_eutopia_institution i
                                                 ON e.publication_affiliation_id = i.scopus_affiliation_id)
   , ref_elsevier_affiliation_other AS (SELECT REPLACE(e.publication_id, 'SCOPUS_ID:', '')  AS article_id,
                                               CONCAT_WS('_',
                                                         a.value ->> 'affiliation_name',
                                                         a.value ->> 'affiliation_city',
                                                         a.value ->> 'affiliation_country') AS affiliation_identifier
                                        FROM src_elsevier_publication_affiliation e,
                                             LATERAL JSONB_ARRAY_ELEMENTS(e.publication_affiliations) AS a)
   , elsevier_institution_other AS (SELECT article_id,
                                           {{ get_eutopia_institution('affiliation_identifier')}} AS institution_id
                                    FROM ref_elsevier_affiliation_other)
   , merged AS (SELECT article_id,
                       article_eid,
                       article_doi,
                       institution_id
                FROM elsevier_affiliation
                UNION ALL
                SELECT e.article_id,
                       e.article_eid,
                       e.article_doi,
                       o.institution_id
                FROM elsevier_affiliation e
                         INNER JOIN elsevier_institution_other o
                                    ON e.article_id = o.article_id)
SELECT DISTINCT article_id,
                article_eid,
                article_doi,
                COALESCE(institution_id, 'n/a') AS institution_id
FROM merged