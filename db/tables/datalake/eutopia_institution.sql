SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS eutopia_institution;

CREATE TABLE eutopia_institution
(
    institution_id           TEXT,
    institution_name         TEXT,
    institution_pretty_name  TEXT,
    institution_country      TEXT,
    institution_language     TEXT,
    institution_country_iso2 TEXT,
    scopus_affiliation_id    TEXT,
    row_created_at           TIMESTAMP,
    task_params_spec         TEXT
);