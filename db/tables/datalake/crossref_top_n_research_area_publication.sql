SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS crossref_top_n_research_area_publication;

CREATE TABLE crossref_top_n_research_area_publication
(
    cerif_research_area_code   TEXT,
    publication_doi      TEXT,
    publication_metadata JSONB,
    row_created_at       TIMESTAMP,
    task_params_spec     TEXT
);