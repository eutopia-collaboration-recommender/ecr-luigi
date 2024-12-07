SET SEARCH_PATH TO jezero;

CREATE TABLE crossref_publication
(
    publication_doi      TEXT,
    publication_metadata JSONB,
    row_created_at       TIMESTAMP,
    task_params_spec     TEXT
);

-- DROP TABLE IF EXISTS crossref_publication;