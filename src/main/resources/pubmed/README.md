# Manual changes required to successfully parse Medline XML

## pubmed_190101.dtd

The `pubmed_190101.dtd` file in the directory was downloaded from: https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
In combination with the `jaxb2-maven-plugin`, the DTD file is used to automatically generate Java classes representing Medline/PubMed records.
Generated classes end up in `target/generated-sources/jaxb`.

The `pubmed_190101.dtd` file in this directory has been modified slightly from the original downloaded version. These modifications were necessary to avoid errors during the automatic Java class generation, e.g.

```
[ERROR] file:/Users/bill/projects/ncats-translator/prototype/tm-pipelines.git/src/main/resources/pubmed/pubmed_190101.dtd [64,-1]
org.xml.sax.SAXParseException: Either an attribute declaration or ">" is expected, not ">"
```

The changes consisted of commenting out line 46 and moving angle brackets on lines 64, 68, 72, 78, and 82 to the previous line so they aren't on a line all by themselves.

## prior to parsing the XML - wrap the article title and abstract text fields in CDATA b/c they contain HTML fragments, <b>. <i>, <u>, <sup>, <sub>

```
sed -i 's/<ArticleTitle\([^>]*\)>/<ArticleTitle\1><![CDATA[/g' pubmed22n1115.xml
sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' pubmed22n1115.xml

sed -i 's/<AbstractText\([^>]*\)>/<AbstractText\1><![CDATA[/g' pubmed22n1115.xml
sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' pubmed22n1115.xml
```