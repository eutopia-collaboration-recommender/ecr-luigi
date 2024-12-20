WITH ref_orcid_article AS (SELECT *
                           FROM {{ ref('stg_orcid_article') }})
   , ref_crossref_article AS (SELECT *
                              FROM {{ ref('stg_crossref_article') }})
   , ref_elsevier_article AS (SELECT *
                              FROM {{ ref('stg_elsevier_article') }})
   , orcid_without_elsevier AS (SELECT *
                                FROM ref_orcid_article
                                WHERE article_eid NOT IN (SELECT article_eid
                                                          FROM ref_elsevier_article))
   , articles AS (SELECT main.article_id,
                         main.article_doi,
                         main.article_eid,
                         c.article_publisher,
                         COALESCE(c.article_title, main.article_title, o.article_title) AS article_title,
                         c.article_short_title,
                         c.article_subtitle,
                         c.article_original_title,
                         COALESCE(c.article_container_title, o.article_journal_title)   AS article_container_title,
                         c.article_short_container_title,
                         COALESCE(c.article_abstract, main.article_abstract)            AS article_abstract,
                         main.article_keywords,
                         COALESCE(c.article_publication_dt, main.article_publication_dt,
                                  o.article_publication_dt)                             AS article_publication_dt,
                         COALESCE(main.article_references, c.article_references)        AS article_references,
                         'elsevier'                                                     AS article_source,
                         ROW_NUMBER() OVER (PARTITION BY main.article_id)               as rn
                  FROM ref_elsevier_article main
                           LEFT JOIN ref_crossref_article c
                                     ON c.article_doi = main.article_doi
                           LEFT JOIN ref_orcid_article o
                                     ON o.article_eid = main.article_eid
                  UNION ALL
                  SELECT main.article_id,
                         main.article_doi,
                         main.article_eid,
                         c.article_publisher,
                         COALESCE(c.article_title, main.article_title)                   AS article_title,
                         c.article_short_title,
                         c.article_subtitle,
                         c.article_original_title,
                         COALESCE(c.article_container_title, main.article_journal_title) AS article_container_title,
                         c.article_short_container_title,
                         c.article_abstract                                              AS article_abstract,
                         null                                                            AS article_keywords,
                         COALESCE(c.article_publication_dt,
                                  main.article_publication_dt)                           AS article_publication_dt,
                         c.article_references                                            AS article_references,
                         'orcid'                                                         AS article_source,
                         ROW_NUMBER() OVER (PARTITION BY main.article_id)                as rn
                  FROM orcid_without_elsevier main
                           LEFT JOIN ref_crossref_article c
                                     ON c.article_doi = main.article_doi
                  )
SELECT article_id,
       article_doi,
       article_eid,
       article_publisher,
       article_title,
       article_short_title,
       article_subtitle,
       article_original_title,
       article_container_title,
       article_short_container_title,
       article_abstract,
       article_keywords,
       article_references,
       article_publication_dt,
       article_source
FROM articles
WHERE rn = 1