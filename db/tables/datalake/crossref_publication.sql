SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS crossref_publication;

CREATE TABLE crossref_publication
(
    publication_doi      TEXT,
    publication_metadata JSONB,
    row_created_at       TIMESTAMP,
    task_params_spec     TEXT
);
