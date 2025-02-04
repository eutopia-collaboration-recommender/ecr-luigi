WITH ref_elsevier_publication_parsed AS (SELECT article_id,
                                                article_doi,
                                                author_id,
                                                author_sequence,
                                                affiliation_id
                                         FROM {{ source('lojze', 'elsevier_publication_parsed') }}),
     ref_stg_elsevier_affiliation AS (SELECT article_id,
                                             institution_id
                                      FROM {{ ref('stg_elsevier_affiliation') }}),
     src_crossref_publication_parsed AS (SELECT DISTINCT article_doi
                                         FROM {{ source('lojze', 'crossref_publication_parsed') }})
SELECT p.article_id,
       p.author_id,
       COALESCE(i.institution_id, 'OTHER') AS institution_id,
       cr.article_doi                      AS crossref_article_doi,
       MAX(p.author_sequence::INT)              AS author_sequence
FROM ref_elsevier_publication_parsed p
         LEFT JOIN ref_stg_elsevier_affiliation i
                   ON p.article_id = i.article_id
         LEFT JOIN src_crossref_publication_parsed cr
                   ON p.article_doi = cr.article_doi
GROUP BY p.article_id, p.author_id, i.institution_id, cr.article_doi
