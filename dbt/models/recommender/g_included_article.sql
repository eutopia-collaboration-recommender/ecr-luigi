SELECT c.article_id
FROM {{ ref('int_collaboration') }} AS c
         INNER JOIN {{ ref("g_included_author") }} AS a
                    ON c.author_id = a.author_id
GROUP BY c.article_id