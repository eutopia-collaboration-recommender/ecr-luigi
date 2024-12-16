SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS top_n_research_area_article_text_embedding;

CREATE TABLE top_n_research_area_article_text_embedding
(
    article_doi            TEXT,
    article_text_embedding FLOAT8[],
    row_created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
