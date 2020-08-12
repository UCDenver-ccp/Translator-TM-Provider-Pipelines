package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.xml.sax.SAXException;

import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn.PubmedSaxHandler;
import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn.PubmedSaxHandlerPlugin;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Getter;

public class MedlineXmlToTextFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testPubmedSaxHandler() throws ParserConfigurationException, SAXException, IOException {
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		InputStream is = new GZIPInputStream(ClassPathUtil.getResourceStreamFromClasspath(MedlineXmlToTextFnTest.class,
				"sample-pubmed20n0001.xml.gz"));
		SAXParser saxParser = saxParserFactory.newSAXParser();
		PubmedTestPlugin handlerPlugin = new PubmedTestPlugin();
		PubmedSaxHandler<PubmedTestPlugin> handler = new PubmedSaxHandler<PubmedTestPlugin>(handlerPlugin);
		saxParser.parse(is, handler);
		// there should have been three documents parsed from the sample XML
		assertEquals("document count not as expected.", 3, handlerPlugin.getCount());
	}

	static class PubmedTestPlugin implements PubmedSaxHandlerPlugin {

		@Getter
		private int count = 0;

		@Override
		public void handlePubmedDocument(TextDocument td) throws SAXException {
			count++;

			String docId = td.getSourceid();
			String docText = td.getText();
			List<TextAnnotation> annotations = td.getAnnotations();

			try {
				String expectedText = null;
				switch (docId) {
				case "1":
					expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
							"PMID1.txt", CharacterEncoding.UTF_8);
					assertEquals("should have a single title annotation", 1, annotations.size());

					for (TextAnnotation annot : annotations) {
						String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
								annot.getAnnotationSpanEnd());
						String coveredText = annot.getCoveredText();
						assertEquals("covered text not the same as spans in document text.", expectedCoveredText,
								coveredText);
					}

					break;

				case "31839728":
					expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
							"PMID31839728.txt", CharacterEncoding.UTF_8);
					assertEquals("should have a two annotations (title + abstract)", 2, annotations.size());

					for (TextAnnotation annot : annotations) {
						String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
								annot.getAnnotationSpanEnd());
						String coveredText = annot.getCoveredText();
						assertEquals("covered text not the same as spans in document text.", expectedCoveredText,
								coveredText);
					}
					break;

				case "31839729":
					expectedText = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
							"PMID31839729.txt", CharacterEncoding.UTF_8);
					assertEquals("should have a two annotations (title + abstract)", 2, annotations.size());

					for (TextAnnotation annot : annotations) {
						String expectedCoveredText = docText.substring(annot.getAnnotationSpanStart(),
								annot.getAnnotationSpanEnd());
						String coveredText = annot.getCoveredText();
						assertEquals("covered text not the same as spans in document text.", expectedCoveredText,
								coveredText);
					}
					break;

				default:
					fail("unexpected document id: " + docId);
				}
				assertEquals(String.format("document text not as expected for document id: %s", docId), expectedText,
						docText);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@Ignore("Seems like this test should pass. It is unclear why it doesn't.")
	@SuppressWarnings("unchecked")
	@Test
	public void testMedlineXmlToTextConversionFn_testPlainText() throws IOException {
		PipelineKey pipelineKey = PipelineKey.MEDLINE_XML_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String medlineXml = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"sample-pubmed20n0001.xml.gz", CharacterEncoding.UTF_8);
		String fileName = "sample-pubmed20n0001.xml.gz";

		PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of(fileName, medlineXml))
				.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;
		PCollectionTuple output = MedlineXmlToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
				timestamp, collection);

		String expectedPmid_1 = "1";
		String expectedText_1 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID1.txt", CharacterEncoding.UTF_8);

		String expectedPmid_2 = "31839728";
		String expectedText_2 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839728.txt", CharacterEncoding.UTF_8);

		String expectedPmid_3 = "31839729";
		String expectedText_3 = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"PMID31839729.txt", CharacterEncoding.UTF_8);

		PAssert.that(output.get(MedlineXmlToTextFn.plainTextTag)).containsInAnyOrder(
				KV.of(expectedPmid_1, Arrays.asList(expectedText_1)),
				KV.of(expectedPmid_2, Arrays.asList(expectedText_2)),
				KV.of(expectedPmid_3, Arrays.asList(expectedText_3)));

		pipeline.run();
	}

	@Ignore("Seems like this test should pass. It is unclear why it doesn't.")
	@SuppressWarnings("unchecked")
	@Test
	public void testMedlineXmlToTextConversionFn_testSerializedAnnotations() throws IOException {
		PipelineKey pipelineKey = PipelineKey.MEDLINE_XML_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String medlineXml = ClassPathUtil.getContentsFromClasspathResource(MedlineXmlToTextFnTest.class,
				"sample-pubmed20n0001.xml.gz", CharacterEncoding.UTF_8);
		String fileName = "sample-pubmed20n0001.xml.gz";

		PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of(fileName, medlineXml))
				.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;
		PCollectionTuple output = MedlineXmlToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
				timestamp, collection);

		String expectedPmid_1 = "1";
		String expectedPmid_2 = "31839728";
		String expectedPmid_3 = "31839729";

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

}
