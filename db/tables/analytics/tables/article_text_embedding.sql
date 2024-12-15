SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS article_text_embedding;

CREATE TABLE article_text_embedding
(
    article_id             TEXT,
    article_text_embedding FLOAT8[],
    row_created_at         TIMESTAMP,
    task_params_spec       TEXT
);
