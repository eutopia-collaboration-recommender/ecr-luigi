WITH src_elsevier_publication AS (SELECT DISTINCT publication_eid
                                  FROM {{ source('lojze', 'elsevier_publication') }}),
     src_orcid_member_works_parsed AS (SELECT article_id,
                                              member_id,
                                              article_doi
                                       FROM {{ source('lojze', 'orcid_member_works_parsed') }}
                                       WHERE article_eid NOT IN
                                             (SELECT DISTINCT publication_eid FROM src_elsevier_publication)),
     src_crossref_publication_parsed AS (SELECT article_doi,
                                                {{ get_eutopia_institution('affiliation_identifier') }} AS institution_id
                                         FROM {{ source('lojze', 'crossref_publication_parsed') }}),
     ref_stg_orcid_member_person AS (SELECT member_id,
                                            member_scopus_id
                                     FROM {{ ref('stg_orcid_member_person') }}),
     ref_stg_orcid_member_employment_by_work AS (SELECT member_id,
                                                        article_id,
                                                        {{ get_eutopia_institution('affiliation_identifier') }} AS institution_id
                                                 FROM {{ ref('stg_orcid_member_employment_by_work') }}),
     merged AS (SELECT o.article_id,
                       o.member_id,
                       c.institution_id,
                       c.article_doi
                FROM src_orcid_member_works_parsed o
                         LEFT JOIN src_crossref_publication_parsed c
                                   ON o.article_doi = c.article_doi
                GROUP BY o.article_id, o.member_id, c.institution_id, c.article_doi
                UNION ALL
                SELECT DISTINCT o.article_id,
                                o.member_id,
                                e.institution_id,
                                c.article_doi
                FROM src_orcid_member_works_parsed o
                         LEFT JOIN src_crossref_publication_parsed c
                                   ON o.article_doi = c.article_doi
                         LEFT JOIN ref_stg_orcid_member_employment_by_work e
                                   ON o.member_id = e.member_id
                                       AND o.article_id = e.article_id
                GROUP BY o.article_id, o.member_id, e.institution_id, c.article_doi),
     orcid_collaboration AS (SELECT DISTINCT m.article_id,
                                             COALESCE(p.member_scopus_id, m.member_id)                                             AS author_id,
                                             COALESCE(m.institution_id, 'n/a')                                                     AS institution_id,
                                             FIRST_VALUE(m.article_doi)
                                             OVER (PARTITION BY m.article_id, COALESCE(p.member_scopus_id, m.member_id), COALESCE(institution_id, 'n/a')
                                                 ORDER BY CASE WHEN m.article_id IS NOT NULL THEN 1 ELSE 0 END DESC, m.article_id) AS article_doi
                             FROM merged m
                                      LEFT JOIN ref_stg_orcid_member_person p
                                                ON m.member_id = p.member_id
                             WHERE m.article_id IS NOT NULL),
     eutopia_orcid_collaboration AS (SELECT article_id
                                     FROM orcid_collaboration
                                     WHERE institution_id NOT IN ('n/a', 'OTHER')
                                     GROUP BY article_id)
SELECT c.article_id,
       c.author_id,
       c.institution_id,
       c.article_doi
FROM orcid_collaboration c
         INNER JOIN eutopia_orcid_collaboration e
                    ON c.article_id = e.article_id
