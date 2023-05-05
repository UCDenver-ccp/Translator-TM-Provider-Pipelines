# CHANGES


## v0.1.2 (May 2023)
* incorporated pipelines for creating input files for the Turku dependency parsing code (to be run via docker via AI-platform) and pipelines to parse the Turku dependency output and store the results in cloud datastore
* airflow scripts related to processing text with the Turku dependency parser
* code to extract dependency paths between entities
* code to index dependency paths (with entities) in elastic search
* a pipeline to derive sentence annotations from the dependency parse output


## v0.1.1 (May 2023)
* updates to handling of embedded HTML tags in the title and abstract text of Medline records
  * adjusted treatment of whitespace around HTML tags
  * added annotations for sub/sup tags to the `sections` document for each PMID


## v0.1.0
* this version represents all development through March 2023


