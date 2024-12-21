SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS collaboration_novelty_metadata;

CREATE TABLE collaboration_novelty_metadata
(
    article_id                       TEXT,
    author_id                        TEXT,
    institution_id                   TEXT,
    article_publication_dt           DATE,
    is_new_author_collaboration      BOOLEAN,
    is_new_institution_collaboration BOOLEAN
);

