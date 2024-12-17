WITH ref_stg_crossref_publication_reference AS (SELECT article_doi,
                                                       reference_article_title,
                                                       reference_article_author,
                                                       reference_article_unstructured,
                                                       reference_article_journal_title,
                                                       ROW_NUMBER() OVER (PARTITION BY article_doi) AS rn
                                                FROM {{ ref('stg_crossref_publication_reference') }})
SELECT article_doi,
       STRING_AGG('Reference:' ||
                  COALESCE(' ' || reference_article_title, '') ||
                  COALESCE(' ' || reference_article_author, '') ||
                  COALESCE(' ' || reference_article_unstructured, '') ||
                  COALESCE(' ' || reference_article_journal_title, '')
           , ', ') AS article_references
FROM ref_stg_crossref_publication_reference
WHERE rn <= 3
GROUP BY article_doi