SELECT e.article_id,
       a.article_citation_normalized_count,
       a.is_eutopia_collaboration,
       a.collaboration_novelty_index,
       e.article_embedding
FROM {{ ref("g_included_article_embedding") }} AS e
         INNER JOIN {{ ref('fct_article') }} AS a
                    ON e.article_id = a.article_id