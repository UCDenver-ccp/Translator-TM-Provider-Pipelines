package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.beam.sdk.io.xml.JAXBCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.medline.MedlineDate;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;
import org.xml.sax.SAXException;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class MedlineXmlToTextFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testTextDocumentConstruction()
			throws ParserConfigurationException, SAXException, IOException, JAXBException, XMLStreamException {
		List<PubmedArticle> articles = getSamplePubmedArticles();
		assertEquals("document count not as expected.", 3, articles.size());
		for (PubmedArticle article : articles) {
			TextDocument td = MedlineXmlToTextFn.buildDocument(article);
			validateDocument(td);
		}
	}

	/**
	 * This method thanks to:
	 * https://stackoverflow.com/questions/10685668/how-to-load-a-relative-system-dtd-into-a-stax-parser
	 * 
	 * @return
	 */
	private static XMLResolver getXmlResolver() {
		return new XMLResolver() {

			@Override
			public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace)
					throws XMLStreamException {

				/*
				 * The systemID argument is the same dtd file specified in the xml file header.
				 * For example, if the xml header is <!DOCTYPE dblp SYSTEM "dblp.dtd">, then
				 * systemID will be "dblp.dtd".
				 * 
				 */
				return Thread.currentThread().getContextClassLoader().getResourceAsStream("pubmed/pubmed_190101.dtd");

			}
		};
	}

	private List<PubmedArticle> getSamplePubmedArticles() throws JAXBException, IOException, XMLStreamException {
		List<PubmedArticle> articles = new ArrayList<PubmedArticle>();
		InputStream is = new GZIPInputStream(ClassPathUtil.getResourceStreamFromClasspath(MedlineXmlToTextFnTest.class,
				"sample-pubmed20n0001.xml.gz"));

		XMLInputFactory xif = XMLInputFactory.newInstance();
		xif.setXMLResolver(getXmlResolver());
		XMLStreamReader xsr = xif.createXMLStreamReader(is);

		JAXBContext jaxbContext = JAXBContext.newInstance(PubmedArticleSet.class);
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		PubmedArticleSet articleSet = (PubmedArticleSet) jaxbUnmarshaller.unmarshal(xsr);

		for (Object article : articleSet.getPubmedArticleOrPubmedBookArticle()) {
			if (article instanceof PubmedArticle) {
				articles.add((PubmedArticle) article);
			} else {
				fail(String.format("Expected only PubmedArticle objects, but observed: %s",
						article.getClass().getName()));
			}
		}
		return articles;
	}

	private void validateDocument(TextDocument td) throws IOException {
		String docId = td.getSourceid();
		String docText = td.getText();
		List<TextAnnotation> annotations = td.getAnnotations();

		String expectedText = null;
		switch (docId) {
		case "PMID:1":
			expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class, "PMID1.txt",
					CharacterEncoding.UTF_8);
			assertEquals("should have a single title annotation", 1, annotations.size());

			for (TextAnnotation annot : annotations) {
				String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
						annot.getAnnotationSpanEnd());
				String coveredText = annot.getCoveredText();
				assertEquals("covered text not the same as spans in document text.", expectedCoveredText, coveredText);
			}

			break;

		case "PMID:31839728":
			expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
					"PMID31839728.txt", CharacterEncoding.UTF_8);
			assertEquals("should have a two annotations (title + abstract)", 2, annotations.size());

			for (TextAnnotation annot : annotations) {
				String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
						annot.getAnnotationSpanEnd());
				String coveredText = annot.getCoveredText();
				assertEquals("covered text not the same as spans in document text.", expectedCoveredText, coveredText);
			}
			break;

		case "PMID:31839729":
			expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
					"PMID31839729.txt", CharacterEncoding.UTF_8);
			assertEquals("should have a two annotations (title + abstract)", 2, annotations.size());

			for (TextAnnotation annot : annotations) {
				String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
						annot.getAnnotationSpanEnd());
				String coveredText = annot.getCoveredText();
				assertEquals("covered text not the same as spans in document text.", expectedCoveredText, coveredText);
			}
			break;

		default:
			fail("unexpected document id: " + docId);
		}
		assertEquals(String.format("document text not as expected for document id: %s", docId), expectedText, docText);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMedlineXmlToTextConversionFn_testPlainText() throws IOException, JAXBException, XMLStreamException {
		PipelineKey pipelineKey = PipelineKey.MEDLINE_XML_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		PCollection<PubmedArticle> input = pipeline
				.apply(Create.of(getSamplePubmedArticles()).withCoder(JAXBCoder.of(PubmedArticle.class)));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
//		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
//				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate empty PCollectionView
		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
				.apply(View.<Set<String>>asSingleton());

		PCollectionTuple output = MedlineXmlToTextFn.process(input, outputTextDocCriteria, timestamp, collection,
				docIdsAlreadyStoredView, OverwriteOutput.YES);

		String expectedPmid_1 = "PMID:1";
		String expectedText_1 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID1.txt", CharacterEncoding.UTF_8);

		String expectedPmid_2 = "PMID:31839728";
		String expectedText_2 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839728.txt", CharacterEncoding.UTF_8);

		String expectedPmid_3 = "PMID:31839729";
		String expectedText_3 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839729.txt", CharacterEncoding.UTF_8);

		PAssert.that(output.get(MedlineXmlToTextFn.plainTextTag)).containsInAnyOrder(
				KV.of(expectedPmid_1, Arrays.asList(expectedText_1)),
				KV.of(expectedPmid_2, Arrays.asList(expectedText_2)),
				KV.of(expectedPmid_3, Arrays.asList(expectedText_3)));

		pipeline.run();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMedlineXmlToTextConversionFn_testSerializedAnnotations()
			throws IOException, JAXBException, XMLStreamException {
		PipelineKey pipelineKey = PipelineKey.MEDLINE_XML_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		PCollection<PubmedArticle> input = pipeline
				.apply(Create.of(getSamplePubmedArticles()).withCoder(JAXBCoder.of(PubmedArticle.class)));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
//		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
//				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate empty PCollectionView
		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
				.apply(View.<Set<String>>asSingleton());

		PCollectionTuple output = MedlineXmlToTextFn.process(input, outputTextDocCriteria, timestamp, collection,
				docIdsAlreadyStoredView, OverwriteOutput.YES);

		String expectedPmid_1 = "PMID:1";
		String expectedPmid_2 = "PMID:31839728";
		String expectedPmid_3 = "PMID:31839729";

		// looks correct, not sure why this doesn't pass
		String expectedSerializedAnnots_1 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID1-sections.bionlp", CharacterEncoding.UTF_8);

		String expectedSerializedAnnots_2 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839728-sections.bionlp", CharacterEncoding.UTF_8);

		String expectedSerializedAnnots_3 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839729-sections.bionlp", CharacterEncoding.UTF_8);

		PAssert.that(output.get(MedlineXmlToTextFn.sectionAnnotationsTag)).containsInAnyOrder(
				KV.of(expectedPmid_1, Arrays.asList(expectedSerializedAnnots_1)),
				KV.of(expectedPmid_2, Arrays.asList(expectedSerializedAnnots_2)),
				KV.of(expectedPmid_3, Arrays.asList(expectedSerializedAnnots_3)));

		pipeline.run();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMedlineXmlToTextConversionFn_testPlainText_someAlreadyStored()
			throws IOException, JAXBException, XMLStreamException {
		PipelineKey pipelineKey = PipelineKey.MEDLINE_XML_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		PCollection<PubmedArticle> input = pipeline
				.apply(Create.of(getSamplePubmedArticles()).withCoder(JAXBCoder.of(PubmedArticle.class)));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
//		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
//				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate PMID:31839728 already has been stored so it should be skipped
//		PCollectionView<Map<String, String>> docIdsAlreadyStoredView = pipeline
//				.apply("Create schema view", Create.of(KV.of("PMID:31839728", "null"))).apply(View.asMap());
//		

		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("PMID:31839728")))
				.apply(View.<Set<String>>asSingleton());

		PCollectionTuple output = MedlineXmlToTextFn.process(input, outputTextDocCriteria, timestamp, collection,
				docIdsAlreadyStoredView, OverwriteOutput.NO);

		String expectedPmid_1 = "PMID:1";
		String expectedText_1 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID1.txt", CharacterEncoding.UTF_8);

//		String expectedPmid_2 = "PMID:31839728";
//		String expectedText_2 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
//				"PMID31839728.txt", CharacterEncoding.UTF_8);

		String expectedPmid_3 = "PMID:31839729";
		String expectedText_3 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839729.txt", CharacterEncoding.UTF_8);

		PAssert.that(output.get(MedlineXmlToTextFn.plainTextTag))
				.containsInAnyOrder(KV.of(expectedPmid_1, Arrays.asList(expectedText_1)),
//				KV.of(expectedPmid_2, Arrays.asList(expectedText_2)),
						KV.of(expectedPmid_3, Arrays.asList(expectedText_3)));

		pipeline.run();
	}

	@Test
	public void testExtractYearFromMedlineDate() {
		MedlineDate md = new MedlineDate();
		md.setvalue("2000 Nov-Dec");
		String extractedYear = MedlineXmlToTextFn.extractYearFromMedlineDate(md);
		assertEquals("2000", extractedYear);

		md = new MedlineDate();
		md.setvalue("1998 Dec-1999 Jan");
		extractedYear = MedlineXmlToTextFn.extractYearFromMedlineDate(md);
		assertEquals("1998", extractedYear);

	}

}
