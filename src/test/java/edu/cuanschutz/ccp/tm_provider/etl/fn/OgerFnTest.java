package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.OgerFn.OgerOutputType;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.SentenceCooccurrenceBuilderTest;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class OgerFnTest {

	private final String TEXT_SAMPLE_1 = "Neurons and blood cells are cells.";
	private final String PUBANNOTATION_SAMPLE_1 = "{\n" + "    \"text\": \"Neurons and blood cells are cells. \",\n"
			+ "    \"denotations\": [\n" + "        {\n" + "            \"id\": \"T1\",\n" + "            \"span\": {\n"
			+ "                \"begin\": 0,\n" + "                \"end\": 7\n" + "            },\n"
			+ "            \"obj\": \"CL:0000540\"\n" + "        },\n" + "        {\n" + "            \"id\": \"T2\",\n"
			+ "            \"span\": {\n" + "                \"begin\": 12,\n" + "                \"end\": 23\n"
			+ "            },\n" + "            \"obj\": \"CL:0000081\"\n" + "        }\n" + "    ],\n"
			+ "    \"sourceid\": \"12345\",\n" + "    \"sourcedb\": \"cord-19\"\n" + "}";
	private final String EXPECTED_BIONLP_SAMPLE_1 = "T1	CL:0000540 0 7	Neurons\n"
			+ "T2	CL:0000081 12 23	blood cells\n";

	@Test
	public void testPubAnnotationToBionlpConversion() throws IOException {
		String bionlp = OgerFn.convertToBioNLP(PUBANNOTATION_SAMPLE_1, "12345", TEXT_SAMPLE_1,
				OgerOutputType.PUBANNOTATION);

		assertEquals(EXPECTED_BIONLP_SAMPLE_1, bionlp);
	}

	@Test
	public void testSpanValidation1() throws IOException {
		String clOgerTsv = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class,
				"PMC1790863.concept_cl.oger.tsv", CharacterEncoding.UTF_8);

		String text = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);

		String bionlp = OgerFn.convertToBioNLP(clOgerTsv, "12345", text, OgerOutputType.TSV);

	}

	
	@Test
	public void testSpanValidation2() throws IOException {
		String clOgerTsv = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class,
				"567.concept_cl.oger.tsv", CharacterEncoding.UTF_8);

		String text = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class, "567.txt",
				CharacterEncoding.UTF_8);

		String bionlp = OgerFn.convertToBioNLP(clOgerTsv, "12345", text, OgerOutputType.TSV);
		System.out.println(bionlp);

	}
	
	@Test
	public void testSpanValidation3() throws IOException {
		String clOgerTsv = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class,
				"7890.concept_cl.oger.tsv", CharacterEncoding.UTF_8);
		
		String text = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class, "7890.txt",
				CharacterEncoding.UTF_8);
		
		String bionlp = OgerFn.convertToBioNLP(clOgerTsv, "12345", text, OgerOutputType.TSV);
		
	}
	
	@Test
	public void testSpanValidation4() throws IOException {
		String clOgerTsv = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class,
				"7890.concept_chebi.oger.tsv", CharacterEncoding.UTF_8);
		
		String text = ClassPathUtil.getContentsFromClasspathResource(OgerFnTest.class, "7890.txt",
				CharacterEncoding.UTF_8);
		
		String bionlp = OgerFn.convertToBioNLP(clOgerTsv, "12345", text, OgerOutputType.TSV);
		
	}
	
	

}
