SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS elsevier_publication_affiliation;

CREATE TABLE elsevier_publication_affiliation
(
    publication_id             TEXT,
    publication_eid            TEXT,
    publication_doi            TEXT,
    publication_affiliation_id TEXT,
    publication_affiliations   JSONB,
    row_created_at             TIMESTAMP,
    task_params_spec           TEXT
);
