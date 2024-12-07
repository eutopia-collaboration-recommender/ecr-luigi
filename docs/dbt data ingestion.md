# dbt data ingestion

**Author:** [@lukazontar](https://github.com/lukazontar)

**Description:** This document outlines the steps that are executed in the dbt data ingestion process.
By running `dbt run --select tag:data_ingestion` we run the staging and intermediate models in the dbt project.

## 1. Parsing source data



## 2. Joining data sources

This document shows how we join different data sources. There's multiple scenarios on how we can join data sources.

1. **Joining Crossref and ORCID data**: since Crossref data is based on DOIs in ORCID data, we can simply join these two
   data
   sources by matching the DOIs in the Crossref data with the DOIs in the ORCID data.
2. **Joining ORCID and Scopus data**: here we can join these two data sources using two different approaches:
    1. **Joining by ORCID**: we can join these two data sources by matching the Scopus author ID in the ORCID data with
       the
       Scopus author ID in the Scopus data. Then we can match articles by the Scopus EID.
    2. **If we do not have the Scopus ID in the ORCID data**, we can assume that the record only exists in one of the
       data sources.
3. Crossref and Scopus data should not be joined directly, since they hold redundant information.

### Establishing identifiers

When joining data sources it is important to establish a common identifier. This identifier should be unique.

Here's how we will define the identifiers for each dimension:

#### DIM Author

- Unique identifier: `coalesce(orcid_member_id, scopus_author_id)`

#### DIM Publication

- Unique identifier: `coalesce(publication_doi, publication_eid, scopus_publication_id)`

#### DIM Affiliation

- Unique identifier: `coalesce(crossref_affiliation_id, scopus_affiliation_id, orcid_employment_id)`
- Connection to EUTOPIA institution, if applicable