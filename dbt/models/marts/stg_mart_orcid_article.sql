WITH ref_stg_mart_orcid_article_with_duplicates AS (SELECT DISTINCT new_article_id AS article_id,
                                                                    article_doi,
                                                                    article_eid,
                                                                    article_title,
                                                                    article_journal_title,
                                                                    article_abstract,
                                                                    article_references,
                                                                    article_citation_count,
                                                                    article_publication_dt
                                                    FROM {{ ref('stg_mart_orcid_article_with_duplicates') }})
SELECT *
FROM ref_stg_mart_orcid_article_with_duplicates