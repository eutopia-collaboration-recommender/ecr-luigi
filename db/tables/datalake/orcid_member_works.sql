SET SEARCH_PATH TO jezero;

CREATE TABLE orcid_member_works
(
    member_id          TEXT,
    member_works       JSONB,
    member_works_count INT,
    row_created_at     TIMESTAMP,
    row_updated_at     TIMESTAMP,
    task_params_spec     TEXT
);

-- DROP TABLE IF EXISTS orcid_member_works;
