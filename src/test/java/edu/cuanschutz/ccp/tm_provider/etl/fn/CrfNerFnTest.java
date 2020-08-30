package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CrfNerFnTest {

	@Test
	public void testAddLeadingColumn() {
		String input = "col1\tline1\n" + "col1\tline2\n" + "col1\tline3\n";
		String expectedOutput = "PMID:1234\tcol1\tline1\n" + "PMID:1234\tcol1\tline2\n" + "PMID:1234\tcol1\tline3\n";

		String output = CrfNerFn.addLeadingColumn(input, "PMID:1234");

		assertEquals(expectedOutput, output);
	}

	@Test
	public void testRemoveFirstColumn() {
		String input = "PMID:1234\tcol1\tline1\n" + "PMID:1234\tcol1\tline2\n" + "PMID:1234\tcol1\tline3\n";
		String expectedOutput = "col1\tline1\n" + "col1\tline2\n" + "col1\tline3\n";

		String output = CrfNerFn.removeFirstColumn(input);

		assertEquals(expectedOutput, output);
	}

	@Test
	public void testExtractBionlp() {
		String input = "{\"docIdToBionlpEntityAnnotationsMap\":{\"PMID:1\":\"T0\tENTITY 45 53\tmethanol\n\"}}";
		String expectedOutput = "T0\tENTITY 45 53\tmethanol\n";
		String output = CrfNerFn.extractBionlp(input);

		assertEquals(expectedOutput, output);
	}

	@Test
	public void testExtractBionlp2() {
		String input = "{\"docIdToBionlpEntityAnnotationsMap\":{}}";
		String expectedOutput = "";
		String output = CrfNerFn.extractBionlp(input);

		assertEquals(expectedOutput, output);
	}

}
