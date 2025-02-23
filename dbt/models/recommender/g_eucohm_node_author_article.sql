WITH ref_g_included_authors AS (SELECT author_id
                                FROM {{ref("g_included_author") }}),
     ref_g_included_articles AS (SELECT article_id
                                 FROM {{ref("g_included_article") }}),
     ref_int_article_citation_normalized AS (SELECT article_id,
                                                    article_citation_normalized_count
                                             FROM {{ref("int_article_citation_normalized") }}),
     ref_stg_collaboration_novelty_index AS (SELECT article_id,
                                                    collaboration_novelty_index
                                             FROM {{ ref('stg_collaboration_novelty_index') }}),
     ref_int_collaboration AS (SELECT c.author_id,
                                      c.article_id,
                                      c.article_publication_dt
                               FROM {{ref('int_collaboration') }} c
                                        INNER JOIN ref_g_included_authors au
                                                   ON c.author_id = au.author_id
                                        INNER JOIN ref_g_included_articles ar
                                                   ON c.article_id = ar.article_id)
SELECT c.author_id,
       c.article_id,
       c.article_publication_dt,
       SUM(cite.article_citation_normalized_count)                                  AS article_citation_normalized_count,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cni.collaboration_novelty_index) AS collaboration_novelty_index
FROM ref_int_collaboration c
         LEFT JOIN ref_int_article_citation_normalized AS cite
                   ON c.article_id = cite.article_id
         LEFT JOIN ref_stg_collaboration_novelty_index AS cni
                   ON c.article_id = cni.article_id
GROUP BY c.author_id,
         c.article_id,
         c.article_publication_dt