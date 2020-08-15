
# pubmed_190101.dtd

The `pubmed_190101.dtd` file in the directory was downloaded from: https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
In combination with the `jaxb2-maven-plugin`, the DTD file is used to automatically generate Java classes representing Medline/PubMed records.
Generated classes end up in `target/generated-sources/jaxb`.

The `pubmed_190101.dtd` file in this directory has been modified slightly from the original downloaded version. These modifications were necessary to avoid errors during the automatic Java class generation, e.g.

```
[ERROR] file:/Users/bill/projects/ncats-translator/prototype/tm-pipelines.git/src/main/resources/pubmed/pubmed_190101.dtd [64,-1]
org.xml.sax.SAXParseException: Either an attribute declaration or ">" is expected, not ">"
```

The changes consisted of removing lines from the original. Note that these lines were not present in the 2018 version of the DTD.

## Lines removed from local version of pubmed_190101.dtd

| Line number | Line text |
| ----------- | --------- |
| 41 | <!-- ============================================================= --> |
| 42 | <!--                     MATHML 3.0 SETUP                        --> |
| 43 | <!-- ============================================================= --> |
| 44 | <!--                    MATHML SETUP FILE                 --> |
| 45 | <!ENTITY % mathml-in-pubmed     SYSTEM        "mathml-in-pubmed.mod"               > |	
| 46 | %mathml-in-pubmed;
| 63 | <!ATTLIST       PubmedArticleSet |
| 64 | > |
| 67 | <!ATTLIST       BookDocumentSet |
| 68 | > |
| 71 | <!ATTLIST       PubmedBookArticleSet |
| 72 | > |
| 77 | <!ATTLIST       PubmedArticle |
| 78 | > |
| 81 | <!ATTLIST       PubmedBookArticle |
| 82 | > |



