CREATE TABLE crossref_publication
(
    publication_doi      TEXT,
    publication_metadata JSONB,
    row_created_at       TIMESTAMP
);

-- DROP TABLE IF EXISTS crossref_publication;