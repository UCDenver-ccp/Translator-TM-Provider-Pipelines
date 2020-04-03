package edu.cuanschutz.ccp.tm_provider.oger.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OgerDictFileFactoryTest {

	
	@Test
	public void testFixLabel() {
		assertEquals("3-ketoacyl-CoA synthase 9",OgerDictFileFactory.fixLabel("3-ketoacyl-CoA synthase 9 (Arabidopsis thaliana)"));
	}
}
