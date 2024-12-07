SET SEARCH_PATH TO jezero;

CREATE TABLE orcid_modified_member
(
    url              VARCHAR(64),
    affiliation      VARCHAR(64),
    member_id        VARCHAR(32),
    host             VARCHAR(32),
    row_created_at   TIMESTAMP,
    task_params_spec TEXT
);

-- DROP TABLE IF EXISTS orcid_modified_member;
