SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS crossref_publication_parsed;

CREATE TABLE crossref_publication_parsed
(
    article_doi                   TEXT,
    article_url                   TEXT,
    article_institution           TEXT,
    article_publisher             TEXT,
    article_title                 TEXT,
    article_short_title           TEXT,
    article_subtitle              TEXT,
    article_original_title        TEXT,
    article_container_title       TEXT,
    article_short_container_title TEXT,
    article_abstract              TEXT,
    article_indexed_dt            DATE,
    article_publication_dt        DATE,
    article_referenced_by_count   INT,
    author_given_name             TEXT,
    author_family_name            TEXT,
    author_sequence               TEXT,
    author_orcid                  TEXT,
    affiliation_identifier        TEXT,
    row_created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
