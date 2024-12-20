WITH ref_elsevier_publication_parsed AS (SELECT article_id,
                                                author_id,
                                                affiliation_id
                                         FROM {{ source('lojze', 'elsevier_publication_parsed') }}
                                         WHERE article_abstract IS NOT NULL),
     ref_stg_elsevier_affiliation AS (SELECT article_id,
                                             institution_id
                                      FROM {{ ref('stg_elsevier_affiliation') }})
SELECT DISTINCT p.article_id,
                p.author_id,
                i.institution_id
FROM ref_elsevier_publication_parsed p
         LEFT JOIN ref_stg_elsevier_affiliation i
                   ON p.article_id = i.article_id
GROUP BY p.article_id, p.author_id, i.institution_id
