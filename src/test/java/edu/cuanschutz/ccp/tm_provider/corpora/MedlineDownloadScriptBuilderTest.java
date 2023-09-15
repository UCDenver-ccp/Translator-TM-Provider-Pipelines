package edu.cuanschutz.ccp.tm_provider.corpora;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MedlineDownloadScriptBuilderTest {

	@Test
	public void testGetIndexStr() {
		assertEquals("0001", MedlineDownloadScriptBuilder.getIndexStr(1));
		assertEquals("0023", MedlineDownloadScriptBuilder.getIndexStr(23));
		assertEquals("0123", MedlineDownloadScriptBuilder.getIndexStr(123));
		assertEquals("1111", MedlineDownloadScriptBuilder.getIndexStr(1111));
	}

}
