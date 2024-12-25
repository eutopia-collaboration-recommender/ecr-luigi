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
     ref_stg_crossref_author_sequence AS (SELECT article_doi,
                                                 author_orcid,
                                                 author_sequence,
                                                 author_sequence_index
                                          FROM {{ ref('stg_crossref_author_sequence') }}),
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
     reworked_ids AS (SELECT m.article_id,
                             COALESCE(p.member_scopus_id, m.member_id) AS author_id,
                             COALESCE(m.institution_id, 'OTHER')       AS institution_id,
                             m.article_doi
                      FROM merged m
                               LEFT JOIN ref_stg_orcid_member_person p
                                         ON m.member_id = p.member_id),
     orcid_collaboration AS (SELECT DISTINCT r.article_id,
                                             r.author_id,
                                             r.institution_id,
                                             FIRST_VALUE(s.author_sequence)
                                             OVER (PARTITION BY r.article_id, r.author_id, r.institution_id
                                                 ORDER BY CASE WHEN s.author_sequence IS NOT NULL THEN 1 ELSE 0 END DESC, s.author_sequence)             AS author_sequence,
                                             MAX(s.author_sequence_index)
                                             OVER (PARTITION BY r.article_id, r.author_id, r.institution_id
                                                 ORDER BY CASE WHEN s.author_sequence_index IS NOT NULL THEN 1 ELSE 0 END DESC, s.author_sequence_index) AS author_sequence_index,
                                             FIRST_VALUE(r.article_doi)
                                             OVER (PARTITION BY r.article_id, r.author_id, r.institution_id
                                                 ORDER BY CASE WHEN r.article_doi IS NOT NULL THEN 1 ELSE 0 END DESC, r.article_id)                      AS article_doi
                             FROM reworked_ids r
                                      LEFT JOIN ref_stg_crossref_author_sequence s
                                                ON r.article_doi = s.article_doi
                                                    AND r.author_id = s.author_orcid
                             WHERE r.article_id IS NOT NULL),
     eutopia_orcid_collaboration AS (SELECT article_id
                                     FROM orcid_collaboration
                                     WHERE institution_id NOT IN ('n/a', 'OTHER')
                                     GROUP BY article_id)
SELECT c.article_id,
       c.author_id,
       c.institution_id,
       c.article_doi,
       c.author_sequence,
       c.author_sequence_index
FROM orcid_collaboration c
         INNER JOIN eutopia_orcid_collaboration e
                    ON c.article_id = e.article_id
