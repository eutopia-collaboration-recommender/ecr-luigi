WITH ref_elsevier_publication_parsed AS (SELECT author_id,
                                                author_first_name,
                                                author_last_name,
                                                author_indexed_name
                                         FROM {{ source('lojze', 'elsevier_publication_parsed') }})
SELECT DISTINCT author_id,
                COALESCE(author_indexed_name,
                         author_first_name || ' ' ||
                         author_last_name) AS author_name
FROM ref_elsevier_publication_parsed