SET SEARCH_PATH TO analitik;

DROP TABLE IF EXISTS author_embedding;

CREATE TABLE author_embedding
(
    author_id             TEXT,
    embedding_tensor_data FLOAT8[],
    row_created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
