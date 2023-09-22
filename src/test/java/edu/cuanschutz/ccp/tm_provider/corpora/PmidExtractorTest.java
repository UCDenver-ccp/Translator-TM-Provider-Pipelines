package edu.cuanschutz.ccp.tm_provider.corpora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;

import edu.cuanschutz.ccp.tm_provider.corpora.PmidExtractor.DocumentTarget;
import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFnTest;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class PmidExtractorTest {
	static final String SAMPLE_PUBMED20N0001_XML_GZ = "sample-pubmed20n0001.xml.gz";
	private static final String SAMPLE_PUBMED_XML_GZ = "sample-pubmed.xml.gz";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	public static List<PubmedArticle> getSamplePubmedArticles(String filename)
			throws JAXBException, IOException, XMLStreamException {
		List<PubmedArticle> articles = new ArrayList<PubmedArticle>();
		InputStream is = new GZIPInputStream(
				ClassPathUtil.getResourceStreamFromClasspath(MedlineXmlToTextFnTest.class, filename));

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

	@Test
	public void testContainsHtml() throws IOException, JAXBException, XMLStreamException {

		File outputDir = folder.newFolder();
		String inputFileName = "input.xml";
		DocumentTarget target = DocumentTarget.CONTAINS_HTML;

		InputStream xmlStream = new GZIPInputStream(
				ClassPathUtil.getResourceStreamFromClasspath(MedlineXmlToTextFnTest.class, SAMPLE_PUBMED_XML_GZ));

		// there is only one document in the input and it contains xml, so one pmid
		// should be extracted
		PmidExtractor.extractPmids(xmlStream, outputDir, inputFileName, target);

		File outputFile = new File(outputDir, inputFileName + "." + target.name().toLowerCase() + ".ids");
		List<String> lines = FileReaderUtil.loadLinesFromFile(outputFile, CharacterEncoding.UTF_8);

		assertEquals(1, lines.size());
		assertEquals("31000267", lines.get(0));

	}

	@Test
	public void testDoesNotContainsHtml() throws IOException, JAXBException, XMLStreamException {

		File outputDir = folder.newFolder();
		String inputFileName = "input.xml";
		DocumentTarget target = DocumentTarget.DOES_NOT_CONTAIN_HTML;

		InputStream xmlStream = new GZIPInputStream(
				ClassPathUtil.getResourceStreamFromClasspath(MedlineXmlToTextFnTest.class, SAMPLE_PUBMED_XML_GZ));

		// there is only one document in the input and it contains xml, so no pmids
		// should be extracted
		PmidExtractor.extractPmids(xmlStream, outputDir, inputFileName, target);

		File outputFile = new File(outputDir, inputFileName + "." + target.name().toLowerCase() + ".ids");
		List<String> lines = FileReaderUtil.loadLinesFromFile(outputFile, CharacterEncoding.UTF_8);

		assertEquals(0, lines.size());

	}

}
