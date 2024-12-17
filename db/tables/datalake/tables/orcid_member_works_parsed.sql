SET SEARCH_PATH TO jezero;

DROP TABLE IF EXISTS elsevier_publication_parsed;

CREATE TABLE orcid_member_works_parsed
(
    member_id                TEXT,
    article_id               TEXT,
    article_doi              TEXT,
    article_eid              TEXT,
    article_title            TEXT,
    article_publication_dt   DATE,
    article_type             TEXT,
    article_last_modified_dt DATE,
    article_journal_title    TEXT
)