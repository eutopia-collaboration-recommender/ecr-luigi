SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS article_keyword_trend;

CREATE TABLE article_keyword_trend
(
    article_keyword   TEXT,
    year              INT,
    publication_count INT,
    row_created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
