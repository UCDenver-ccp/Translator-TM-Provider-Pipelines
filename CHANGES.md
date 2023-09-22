## v0.3.0 (Sep 2023)

- concept recognition pipeline refresh
  - updates based on comprehensive error analysis and updated ontologies
  - incorporation of abbreviation information
  - various pipeline fixes


## v0.2.1 (Aug 2023)

- incorporated pipelines for creating input files for the Turku dependency parsing code (to be run via docker via AI-platform) and pipelines to parse the Turku dependency output and store the results in cloud datastore
- also added Dockerfile for running the Turku English dependency parser on AI-platform

## v0.2.0

- updates to publication metadata code
  - export of months and days is now more standardized
    - months are exported as their 3-letter abbreviation
    - days are exported a 2-digit strings, e.g. 03
  - there is now a dedicated airflow pipeline for keeping the pub metadata api up-to-date

## v0.1.1 (May 2023)

- updates to handling of embedded HTML tags in the title and abstract text of Medline records
  - adjusted treatment of whitespace around HTML tags
  - added annotations for sub/sup tags to the `sections` document for each PMID

## v0.1.0

- this version represents all development through March 2023
