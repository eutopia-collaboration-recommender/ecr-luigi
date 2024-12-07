CREATE TABLE eutopia_institution
(
    institution_id           TEXT,
    institution_name         TEXT,
    institution_pretty_name  TEXT,
    institution_country      TEXT,
    institution_language     TEXT,
    institution_country_iso2 TEXT,
    _sql_string_condition    TEXT,
    _python_string_condition TEXT,
    row_created_at           TIMESTAMP,
    task_params_spec         TEXT
);