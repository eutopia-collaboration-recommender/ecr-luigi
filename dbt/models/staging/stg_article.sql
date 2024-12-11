WITH ref_stg_orcid_member_work AS (SELECT *
                                   FROM {{ ref('stg_orcid_article')}})
   , ref_stg_crossref_publication AS (SELECT *
                                      FROM {{ ref('stg_crossref_article')}})
   , ref_stg_elsevier_publication AS (SELECT *
                                      FROM {{ ref('stg_elsevier_article')}})
   , ref_stg_collaboration AS (SELECT DISTINCT article_id,
                                               article_doi,
                                               article_eid,
                                               elsevier_article_id,
                                               orcid_article_id,
                                               source
                               FROM {{ ref('stg_collaboration')}})
   , merged AS (SELECT main.article_id,
                       main.article_doi,
                       main.article_eid,
                       c.article_publisher,
                       COALESCE(c.article_title, e.article_title, o.article_title)  AS article_title,
                       c.article_short_title,
                       c.article_subtitle,
                       c.article_original_title,
                       COALESCE(c.article_container_title, o.article_journal_title) AS article_container_title,
                       c.article_short_container_title,
                       COALESCE(c.article_abstract, e.article_abstract)             AS article_abstract,
                       e.article_keywords,
                       COALESCE(c.article_publication_dt, e.article_publication_dt,
                                o.article_publication_dt)                           AS article_publication_dt,
                       main.source                                                  AS article_source
                FROM ref_stg_collaboration main
                         LEFT JOIN ref_stg_crossref_publication c
                                   ON c.article_doi = main.article_doi
                         LEFT JOIN ref_stg_elsevier_publication e
                                   ON e.article_id = main.elsevier_article_id
                         LEFT JOIN ref_stg_orcid_member_work o
                                   ON o.article_id = main.orcid_article_id)
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
       article_publication_dt,
       article_source
FROM merged
