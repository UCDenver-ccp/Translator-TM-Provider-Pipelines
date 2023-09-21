package edu.cuanschutz.ccp.tm_provider.corpora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import jakarta.xml.bind.JAXBException;
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
				+ "PMID:31000267	2019	Jun	19	Food research international (Ottawa, Ont.)	Food Res Int	120	-	In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.	The antioxidant capacity of red tilapia viscera hydrolysates (RTVH) with different degrees of hydrolysis (DH) as well as their ultrafiltration membrane fractions, were analyzed using different chemical assays. Their protective effects against oxidative stress were evaluated using H2O2-stressed human intestinal differentiated Caco-2. The highest antioxidant capacity was obtained with a DH of 42.5% (RTVH-A) and its <1 kDa fraction (FRTVH-V). RTVH-A and FRTVH-V did not show cytotoxic effects at a concentration of ≤0.5 mg/mL,prevented the decrease in cell viability, and suppressed intracellular reactive oxygen species (ROS) accumulation induced by H2O2. However, pretreatment with RTVH-A after adding H2O2, showed a greater decrease in glutathione levels. Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis. These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.\n";

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
				+ "PMID:1	1975	Jun	-	Biochemical medicine	Biochem Med	13	2	Formate assay in body fluids: application in methanol poisoning.	-\n"
				+ "PMID:31839728	2020	Feb	15	Biochemia medica	Biochem Med (Zagreb)	30	1	High anion gap metabolic acidosis caused by D-lactate: mind the time of blood collection.	D-lactic acidosis is an uncommon cause of high anion gap acidosis. A 35-year old woman was admitted to the emergency room with somnolence, drowsiness, dizziness, incoherent speech and drunk appearance. Her past medical history included a Roux-en-Y bypass. Point-of-care venous blood analysis revealed a high anion gap acidosis. Based on the clinical presentation, routine laboratory results and negative toxicology screening, D-lactate and 5-oxoprolinuria were considered as the most likely causes of the high anion gap acidosis. Urine organic acid analysis revealed increased lactate, but no 5-oxoproline. Plasma D-lactate was < 1.0 mmol/L and could not confirm D-lactic acidosis. Further investigation revealed that the blood sample for D-lactate was drawn 12 hours after admission, which might explain the false-negative result. Data regarding the half-life of D-lactate are, however, scarce. During a second admission, one month later, D-lactic acidosis could be confirmed with an anion gap of 40.7 mmol/L and a D-lactate of 21.0 mmol/L measured in a sample collected at the time of admission. The time of blood collection is of utmost importance to establish the diagnosis of D-lactic acidosis due to the fast clearance of D-lactate in the human body.\n"
				+ "PMID:31839729	2020	Feb	15	Biochemia medica	Biochem Med (Zagreb)	30	1	Unexpected abnormal coagulation test results in a 2-year-old child: A case report.	Rejection of the sample with repeated blood withdrawal is always an unwanted consequence of sample nonconformity and preanalytical errors, especially in the most vulnerable population - children. Here is presented a case with unexpected abnormal coagulation test results in a 2-year-old child with no previously documented coagulation disorder. Child is planned for tympanostomy tubes removal under the anaesthesia driven procedure, and preoperative coagulation tests revealed prolonged prothrombin time, activated partial thromboplastin time and thrombin time, with fibrinogen and antithrombin within reference intervals. From the anamnestic and clinical data, congenital coagulation disorder was excluded, and with further investigation, sample mismatch, clot presence and accidental ingestion of oral anticoagulant, heparin contamination or vitamin K deficiency were excluded too. Due to suspected EDTA carryover during blood sampling another sample was taken the same day and all tests were performed again. The results for all tests were within reference intervals confirming EDTA effect on falsely prolongation of the coagulation times in the first sample. This case can serve as alert to avoid unnecessary loss in terms of blood withdrawal repetitions and discomfort of the patients and their relatives, tests repeating, prolonging medical procedures, and probably delaying diagnosis or proper medical treatment. It is the responsibility of the laboratory specialists to continuously educate laboratory staff and other phlebotomists on the correct blood collection as well as on its importance for the patient's safety.\n";

		try (BufferedReader reader = FileReaderUtil.initBufferedReader(
				new GZIPInputStream(new FileInputStream(expectedOutputFile)), CharacterEncoding.UTF_8)) {
			String content = IOUtils.toString(reader);
			assertEquals(expectedContent, content);
		}

	}

	@Test
	public void testGetMonth() {
		String month = MedlineUiMetadataExtractor.getMonth("1998 Dec-1999 Jan", "1234");
		assertEquals("Dec", month);

		month = MedlineUiMetadataExtractor.getMonth("2015 Nov-Dec", "1234");
		assertEquals("Nov", month);

		month = MedlineUiMetadataExtractor.getMonth("2009 Dec-2010 Jan", "1234");
		assertEquals("Dec", month);

		month = MedlineUiMetadataExtractor.getMonth("Spring 2017", "1234");
		assertEquals("Apr", month);

		month = MedlineUiMetadataExtractor.getMonth("2017 Mai", "1234");
		assertEquals("May", month);

		month = MedlineUiMetadataExtractor.getMonth("2017 MAY", "1234");
		assertEquals("May", month);

		month = MedlineUiMetadataExtractor.getMonth("1975-1976", "1234");
		assertEquals(null, month);

		month = MedlineUiMetadataExtractor.getMonth("1976-1977 Winter", "1234");
		assertEquals("Jan", month);

		month = MedlineUiMetadataExtractor.getMonth("1977-1978 Fall-Winter", "1234");
		assertEquals("Oct", month);

		month = MedlineUiMetadataExtractor.getMonth("2002 Winter-Spring", "1234");
		assertEquals("Jan", month);

		month = MedlineUiMetadataExtractor.getMonth("1983 Fall-Winter", "1234");
		assertEquals("Oct", month);

		month = MedlineUiMetadataExtractor.getMonth("1983-1984 Winter-Spring", "1234");
		assertEquals("Jan", month);

		month = MedlineUiMetadataExtractor.getMonth("2017 1-2", "1234");
		assertEquals(null, month);

		month = MedlineUiMetadataExtractor.getMonth("2015 -2016", "1234");
		assertEquals(null, month);

		month = MedlineUiMetadataExtractor.getMonth("2023 Special Issue On COVID-19", "1234");
		assertEquals(null, month);

		month = MedlineUiMetadataExtractor.getMonth("2012 Autumn-2013 Winter", "1234");
		assertEquals("Oct", month);

		month = MedlineUiMetadataExtractor.getMonth("2023 First Quarter", "1234");
		assertEquals("Jan", month);

		month = MedlineUiMetadataExtractor.getMonth("2022 Third Quarter", "1234");
		assertEquals("Jul", month);

		month = MedlineUiMetadataExtractor.getMonth("2022 avril", "1234");
		assertEquals("Apr", month);

		month = MedlineUiMetadataExtractor.getMonth("2022 juillet", "1234");
		assertEquals("Jul", month);

		month = MedlineUiMetadataExtractor.getMonth("2023 Spring 01", "1234");
		assertEquals("Apr", month);

		month = MedlineUiMetadataExtractor.getMonth("2022 décembre 01", "1234");
		assertEquals("Dec", month);

		month = MedlineUiMetadataExtractor.getMonth("2019 Spring/Summer", "1234");
		assertEquals("Apr", month);

		month = MedlineUiMetadataExtractor.getMonth("1965 3d Quart", "1234");
		assertEquals("Jul", month);

		month = MedlineUiMetadataExtractor.getMonth("2020 Supplement Jan", "1234");
		assertEquals("Jan", month);

	}
	
	@Test
	public void testSingle() {
		String month = MedlineUiMetadataExtractor.getMonth("2023 First Quarter", "1234");
		assertEquals("Jan", month);
	}

	@Test
	public void testGetDay() {
		String day = MedlineUiMetadataExtractor.getDay("2008 Dec 1-8", "1234");
		assertEquals("1", day);

		day = MedlineUiMetadataExtractor.getDay("2009 Dec-2010 Jan", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2017 Mai", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("1975-1976", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("1976-1977 Winter", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("1977-1978 Fall-Winter", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2002 Winter-Spring", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("1983 Fall-Winter", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("1983-1984 Winter-Spring", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2017 1-2", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2015 -2016", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2023 Special Issue On COVID-19", "1234");
		assertEquals(null, day);

		day = MedlineUiMetadataExtractor.getDay("2012 Autumn-2013 Winter", "1234");
		assertEquals(null, day);

	}

	@Test
	public void testPattern() {
		Pattern p = Pattern.compile(
				"^\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)-\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)$");
		Matcher m = p.matcher("2009 Dec-2010 Jan");
		assertTrue(m.find());

	}

}
