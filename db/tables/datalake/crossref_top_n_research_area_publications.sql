SET SEARCH_PATH TO jezero;

CREATE TABLE crossref_top_n_research_area_publications
(
    research_area_code   TEXT,
    publication_doi      TEXT,
    publication_metadata JSONB,
    row_created_at       TIMESTAMP,
    task_params_spec     TEXT
);