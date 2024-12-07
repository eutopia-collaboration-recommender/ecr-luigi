SET SEARCH_PATH TO jezero;

CREATE TABLE orcid_member_person
(
    member_id TEXT,
    member_person JSONB,
    row_created_at     TIMESTAMP,
    row_updated_at     TIMESTAMP,
    task_params_spec     TEXT
);

-- DROP TABLE IF EXISTS orcid_member_person;
