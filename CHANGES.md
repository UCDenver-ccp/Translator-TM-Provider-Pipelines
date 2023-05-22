## v0.2.0
  * updates to publication metadata code
    * export of months and days is now more standardized
      * months are exported as their 3-letter abbreviation
      * days are exported a 2-digit strings, e.g. 03
    * there is now a dedicated airflow pipeline for keeping the pub metadata api up-to-date


## v0.1.1
  * updates to handling of embedded HTML tags in the title and abstract text of Medline records
    * adjusted treatment of whitespace around HTML tags
    * added annotations for sub/sup tags to the `sections` document for each PMID