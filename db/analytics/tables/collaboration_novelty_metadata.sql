SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS collaboration_novelty_metadata;

CREATE TABLE collaboration_novelty_metadata
(
    article_id                        TEXT,
    author_id                         TEXT,
    institution_id                    TEXT,
    article_publication_dt            DATE,
    has_new_author_collaboration      BOOLEAN,
    has_new_institution_collaboration BOOLEAN,
    row_created_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

