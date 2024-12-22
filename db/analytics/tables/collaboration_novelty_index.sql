SET SEARCH_PATH TO analitik;


DROP TABLE IF EXISTS collaboration_novelty_index;

CREATE TABLE collaboration_novelty_index
(
    article_id                  TEXT,
    collaboration_novelty_index FLOAT8,
    row_created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

