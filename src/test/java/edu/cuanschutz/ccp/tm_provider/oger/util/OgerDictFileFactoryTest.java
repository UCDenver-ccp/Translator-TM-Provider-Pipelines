package edu.cuanschutz.ccp.tm_provider.oger.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OgerDictFileFactoryTest {

	@Test
	public void testIsCaseSensitive() {
		assertTrue(OgerDictFileFactory.isCaseSensitive("WAS"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("TNF-α"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("SD3"));
		assertTrue(OgerDictFileFactory.isCaseSensitive("SD2a"));
		assertFalse(OgerDictFileFactory.isCaseSensitive("Wiskott-Aldrich syndrome"));
	}

	@Test
	public void testFixLabel() {
		assertEquals("3-ketoacyl-CoA synthase 9",
				OgerDictFileFactory.fixLabel("3-ketoacyl-CoA synthase 9 (Arabidopsis thaliana)"));
		
		assertEquals("Unclassified Pleomorphic sarcoma", OgerDictFileFactory.fixLabel("Unclassified Pleomorphic sarcoma (formerly \\\"malignant fibrous histiocytoma\\\")\""));
	}
}
