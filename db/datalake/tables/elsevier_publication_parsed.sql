SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS elsevier_publication_parsed;

CREATE TABLE elsevier_publication_parsed
(
    article_id             TEXT,
    article_eid            TEXT,
    article_doi            TEXT,
    article_title          TEXT,
    article_journal        TEXT,
    article_type           TEXT,
    article_abstract       TEXT,
    article_citation_count INT,
    article_publication_dt DATE,
    author_id              TEXT,
    author_initials        TEXT,
    author_last_name       TEXT,
    author_first_name      TEXT,
    author_indexed_name    TEXT,
    author_sequence        TEXT,
    is_first_author        BOOLEAN,
    affiliation_id         TEXT,
    article_keywords       TEXT,
    row_created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
