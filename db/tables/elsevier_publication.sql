CREATE TABLE elsevier_publication
(
    publication_id                   TEXT,
    publication_eid                  TEXT,
    publication_doi                  TEXT,
    publication_title                TEXT,
    publication_type                 TEXT,
    publication_abstract             TEXT,
    publication_citation_count       INT,
    publication_dt                   DATE,
    publication_last_modification_dt DATE,
    publication_authors              JSONB,
    publication_keywords             JSONB,
    publication_references           JSONB,
    row_created_at                   TIMESTAMP
);

-- DROP TABLE IF EXISTS elsevier_publication;