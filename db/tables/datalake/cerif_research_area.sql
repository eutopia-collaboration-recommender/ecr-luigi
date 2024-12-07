SET SEARCH_PATH TO jezero;

CREATE TABLE crossref_top_n_research_area_publications
(
    publication_doi          TEXT,
    publication_metadata     JSONB,
    cerif_research_area_code TEXT,
    row_created_at           TIMESTAMP,
    task_params_spec         TEXT
);