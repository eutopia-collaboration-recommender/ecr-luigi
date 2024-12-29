SELECT e.article_id,
       a.article_citation_normalized_count,
       CASE WHEN a.is_eutopia_collaboration THEN 1 ELSE 0 END AS is_eutopia_collaboration,
       a.collaboration_novelty_index,
       e.article_embedding
FROM {{ ref("g_included_article_embedding") }} AS e
         INNER JOIN {{ ref('fct_article') }} AS a
                    ON e.article_id = a.article_id