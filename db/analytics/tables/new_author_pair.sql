SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS new_author_pair;

CREATE TABLE new_author_pair
(
    article_id         TEXT,
    author_id          TEXT,
    co_author_id       TEXT,
    is_new_author_pair BOOLEAN,
    row_created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);