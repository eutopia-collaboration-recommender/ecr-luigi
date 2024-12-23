SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS article_research_area;

CREATE TABLE article_research_area
(
    article_id         TEXT,
    research_area_code TEXT,
    research_area_rank INTEGER,
    row_created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
