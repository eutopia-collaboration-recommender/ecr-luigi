-- CREATE DATABASE lojze;

CREATE SCHEMA jezero;
CREATE SCHEMA analitik;

CREATE INDEX crossref_publication_idx_json ON crossref_publication USING gin (publication_metadata);
 create