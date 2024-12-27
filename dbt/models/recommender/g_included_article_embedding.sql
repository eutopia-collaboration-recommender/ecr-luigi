SELECT e.article_id,
       e.article_text_embedding AS article_embedding
FROM {{ source('analitik', 'article_text_embedding')}} e
         INNER JOIN {{ ref("g_included_article") }} AS a
                    ON e.article_id = a.article_id