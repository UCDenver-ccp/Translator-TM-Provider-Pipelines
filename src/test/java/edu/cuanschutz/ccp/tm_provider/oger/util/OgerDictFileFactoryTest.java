package edu.cuanschutz.ccp.tm_provider.oger.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class OgerDictFileFactoryTest {

	@Test
	public void testIsCaseSensitive() {
		assertTrue(OgerDictFileFactory.isCaseSensitive("WAS"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("TNF-α"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("SD3"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("SD2a"));
		assertFalse(OgerDictFileFactory.isCaseSensitive("Wiskott-Aldrich syndrome"));
		assertFalse(OgerDictFileFactory.isCaseSensitive("triglyceride lipase activity"));

		assertTrue(OgerDictFileFactory.isCaseSensitive("casE"));

		assertTrue(OgerDictFileFactory.isCaseSensitive("TAG activity"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("NO binding"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("NOS binding"));
	}

	@Test
	public void testFixLabel() {
		assertEquals("3-ketoacyl-CoA synthase 9",
				OgerDictFileFactory.fixLabel("3-ketoacyl-CoA synthase 9 (Arabidopsis thaliana)"));

		assertEquals("Unclassified Pleomorphic sarcoma", OgerDictFileFactory
				.fixLabel("Unclassified Pleomorphic sarcoma (formerly \\\"malignant fibrous histiocytoma\\\")\""));

		assertEquals("hu(P1)", OgerDictFileFactory.fixLabel("hu(P1)"));
	}

	@Test
	public void testGetCaseSensitiveSynonyms() {
		Set<String> synonyms = new HashSet<String>(Arrays.asList("BRCA1"));
		Set<String> csSynonyms = OgerDictFileFactory.getCaseSensitiveSynonyms(synonyms);
		Set<String> expectedCsSynonyms = new HashSet<String>(Arrays.asList("BRCA1", "Brca1"));
		assertEquals(expectedCsSynonyms, csSynonyms);

		synonyms = new HashSet<String>(Arrays.asList("RAD51"));
		csSynonyms = OgerDictFileFactory.getCaseSensitiveSynonyms(synonyms);
		expectedCsSynonyms = new HashSet<String>(Arrays.asList("RAD51", "Rad51"));
		assertEquals(expectedCsSynonyms, csSynonyms);

		synonyms = new HashSet<String>(Arrays.asList("TNFβ"));
		csSynonyms = OgerDictFileFactory.getCaseSensitiveSynonyms(synonyms);
		expectedCsSynonyms = new HashSet<String>(Arrays.asList("TNFβ"));
		assertEquals(expectedCsSynonyms, csSynonyms);

	}
}
