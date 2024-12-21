SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS article_keywords;

CREATE TABLE article_keywords
(
    article_id       TEXT,
    article_keywords TEXT[],
    row_created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
