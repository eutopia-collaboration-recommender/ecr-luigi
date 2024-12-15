WITH ref_stg_orcid_member_work AS (SELECT *
                                   FROM {{ ref('stg_orcid_member_work') }})
   , ref_stg_orcid_member_person AS (SELECT *
                                     FROM {{ ref('stg_orcid_member_person') }})
   , ref_stg_orcid_member_employment_by_work AS (SELECT *
                                                 FROM {{ ref('stg_orcid_member_employment_by_work') }})
   , src_crossref_publication AS (SELECT *
                                  FROM {{ source('lojze', 'crossref_publication') }})
   , src_elsevier_publication AS (SELECT *
                                  FROM {{ source('lojze', 'elsevier_publication') }})
   , ref_stg_elsevier_affiliation AS (SELECT *
                                      FROM {{ ref('stg_elsevier_affiliation') }})
SELECT o.article_id              AS article_id,
       o.article_doi,
       o.article_eid,
       o.article_id              AS orcid_article_id,
       o.member_id               AS orcid_author_id,
       o.article_publication_dt  AS orcid_publication_dt,
       oe.affiliation_identifier AS orcid_affiliation_identifier,
       c.article_publication_dt  AS crossref_publication_dt,
       c.affiliation_identifier  AS crossref_affiliation_identifier,
       e.article_id              AS elsevier_article_id,
       e.article_publication_dt  AS elsevier_publication_dt,
       e.author_id               AS elsevier_author_id,
       ea.affiliation_identifier AS elsevier_affiliation_identifier,
       'orcid'                   AS source
FROM ref_stg_orcid_member_work o
         LEFT JOIN ref_stg_orcid_member_person op
                   ON o.member_id = op.member_id
         LEFT JOIN ref_stg_orcid_member_employment_by_work oe
                   ON oe.member_id = o.member_id
                       AND oe.article_id = o.article_id
    -- Join with Crossref
         LEFT JOIN src_crossref_publication c
                   ON o.article_doi = c.article_doi
    -- Join with Elsevier
         LEFT JOIN src_elsevier_publication e
                   ON o.article_eid = e.article_eid
                       AND op.member_scopus_id = e.author_id
         LEFT JOIN ref_stg_elsevier_affiliation ea
                   ON ea.article_eid = o.article_eid
UNION ALL
SELECT e.article_id              AS article_id,
       e.article_doi,
       e.article_eid,
       o.article_id              AS orcid_article_id,
       op.member_id              AS orcid_author_id,
       o.article_publication_dt  AS orcid_publication_dt,
       oe.affiliation_identifier AS orcid_affiliation_identifier,
       c.article_publication_dt  AS crossref_publication_dt,
       c.affiliation_identifier  AS crossref_affiliation_identifier,
       e.article_id              AS elsevier_article_id,
       e.article_publication_dt  AS elsevier_publication_dt,
       e.author_id               AS elsevier_author_id,
       ea.affiliation_identifier AS elsevier_affiliation_identifier,
       'elsevier'                AS source
FROM src_elsevier_publication e
         LEFT JOIN ref_stg_elsevier_affiliation ea
                   ON ea.article_id = e.article_id
         LEFT JOIN ref_stg_orcid_member_person op
                   ON op.member_scopus_id = e.author_id
         LEFT JOIN ref_stg_orcid_member_work o
                   ON o.article_eid = e.article_eid
                       AND o.member_id = op.member_id
         LEFT JOIN ref_stg_orcid_member_employment_by_work oe
                   ON oe.member_id = o.member_id
                       AND oe.article_id = o.article_id
         LEFT JOIN src_crossref_publication c
                   ON e.article_doi = c.article_doi

