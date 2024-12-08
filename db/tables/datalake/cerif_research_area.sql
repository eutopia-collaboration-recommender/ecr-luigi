SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS cerif_research_area;

CREATE TABLE cerif_research_area
(
    research_branch_code    TEXT,
    research_branch_name    TEXT,
    research_subbranch_code TEXT,
    research_subbranch_name TEXT,
    research_area_code      TEXT,
    research_area_name      TEXT,
    row_created_at          TIMESTAMP,
    task_params_spec        TEXT
);
