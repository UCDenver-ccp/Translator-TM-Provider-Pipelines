package edu.cuanschutz.ccp.tm_provider.corpora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFnTest;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class MedlineUiMetadataExtractorTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testExtract() throws IOException, JAXBException, XMLStreamException {

		File outputDirectory = folder.newFolder("output");
		String inputFileName = "medline03n1111.xml.gz";

		InputStream xmlStream = new GZIPInputStream(ClassPathUtil.getResourceStreamFromClasspath(
				MedlineXmlToTextFnTest.class, MedlineXmlToTextFnTest.SAMPLE_PUBMED_XML_GZ));

		MedlineUiMetadataExtractor.extract(xmlStream, outputDirectory, inputFileName);

		File expectedOutputFile = new File(outputDirectory, "medline03n1111.xml.gz.ui_metadata.tsv.gz");
		assertTrue(expectedOutputFile.exists());

		String expectedContent = "DOC_ID	year	month	day	journal	journal_abbrev	volume	issue	article_title	article_abstract\n"
				+ "PMID:31000267	2019	06	19	Food research international (Ottawa, Ont.)	Food Res Int	120	-	In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.	The antioxidant capacity of red tilapia viscera hydrolysates (RTVH) with different degrees of hydrolysis (DH) as well as their ultrafiltration membrane fractions, were analyzed using different chemical assays. Their protective effects against oxidative stress were evaluated using H2O2-stressed human intestinal differentiated Caco-2. The highest antioxidant capacity was obtained with a DH of 42.5% (RTVH-A) and its <1 kDa fraction (FRTVH-V). RTVH-A and FRTVH-V did not show cytotoxic effects at a concentration of ≤0.5 mg/mL,prevented the decrease in cell viability, and suppressed intracellular reactive oxygen species (ROS) accumulation induced by H2O2. However, pretreatment with RTVH-A after adding H2O2, showed a greater decrease in glutathione levels. Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis. These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.\n";

		try (BufferedReader reader = FileReaderUtil.initBufferedReader(
				new GZIPInputStream(new FileInputStream(expectedOutputFile)), CharacterEncoding.UTF_8)) {
			String content = IOUtils.toString(reader);
			System.out.println(content);
			assertEquals(expectedContent, content);
		}

		File expectedDeleteFile = new File(outputDirectory, "medline03n1111.xml.gz.ui_metadata.delete.tsv.gz");
		assertTrue(expectedDeleteFile.exists());

		String expectedDeleteContent = "PMID:35184107\n" + "PMID:35657607\n";

		try (BufferedReader reader = FileReaderUtil.initBufferedReader(
				new GZIPInputStream(new FileInputStream(expectedDeleteFile)), CharacterEncoding.UTF_8)) {
			String content = IOUtils.toString(reader);
			System.out.println(content);
			assertEquals(expectedDeleteContent, content);
		}

	}

	@Test
	public void testExtract2() throws IOException, JAXBException, XMLStreamException {

		File outputDirectory = folder.newFolder("output");
		String inputFileName = "medline03n1111.xml.gz";

		InputStream xmlStream = new GZIPInputStream(ClassPathUtil.getResourceStreamFromClasspath(
				MedlineXmlToTextFnTest.class, MedlineXmlToTextFnTest.SAMPLE_PUBMED20N0001_XML_GZ));

		MedlineUiMetadataExtractor.extract(xmlStream, outputDirectory, inputFileName);

		File expectedOutputFile = new File(outputDirectory, "medline03n1111.xml.gz.ui_metadata.tsv.gz");
		assertTrue(expectedOutputFile.exists());

		String expectedContent = "DOC_ID	year	month	day	journal	journal_abbrev	volume	issue	article_title	article_abstract\n"
				+ "PMID:31000267	2019	06	19	Food research international (Ottawa, Ont.)	Food Res Int	120	-	In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.	The antioxidant capacity of red tilapia viscera hydrolysates (RTVH) with different degrees of hydrolysis (DH) as well as their ultrafiltration membrane fractions, were analyzed using different chemical assays. Their protective effects against oxidative stress were evaluated using H2O2-stressed human intestinal differentiated Caco-2. The highest antioxidant capacity was obtained with a DH of 42.5% (RTVH-A) and its <1 kDa fraction (FRTVH-V). RTVH-A and FRTVH-V did not show cytotoxic effects at a concentration of ≤0.5 mg/mL,prevented the decrease in cell viability, and suppressed intracellular reactive oxygen species (ROS) accumulation induced by H2O2. However, pretreatment with RTVH-A after adding H2O2, showed a greater decrease in glutathione levels. Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis. These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.\n";

		try (BufferedReader reader = FileReaderUtil.initBufferedReader(
				new GZIPInputStream(new FileInputStream(expectedOutputFile)), CharacterEncoding.UTF_8)) {
			String content = IOUtils.toString(reader);
//			assertEquals(expectedContent, content);
			System.out.println(content);
		}

	}

	@Test
	public void testGetMonth() {
		String month = MedlineUiMetadataExtractor.getMonth("1998 Dec-1999 Jan");
		assertEquals("12", month);

		month = MedlineUiMetadataExtractor.getMonth("2015 Nov-Dec");
		assertEquals("11", month);
	}

}
