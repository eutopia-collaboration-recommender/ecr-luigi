SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS article_language;

CREATE TABLE article_language
(
    article_id       TEXT,
    article_language TEXT,
    row_created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
