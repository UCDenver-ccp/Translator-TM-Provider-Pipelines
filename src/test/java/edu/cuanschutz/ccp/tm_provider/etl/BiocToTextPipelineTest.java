package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BiocToTextPipelineTest {

	@Test
	public void testSubCollectionNameBasedOnId() {
		assertEquals("PMC_SUBSET_0", BiocToTextPipeline.getSubCollectionName("PMC13900"));
		assertEquals("PMC_SUBSET_31", BiocToTextPipeline.getSubCollectionName("PMC7813158"));
		assertEquals("PMC_SUBSET_0", BiocToTextPipeline.getSubCollectionName("PMC249999"));
		assertEquals("PMC_SUBSET_1", BiocToTextPipeline.getSubCollectionName("PMC250000"));
		assertEquals("PMC_SUBSET_1", BiocToTextPipeline.getSubCollectionName("PMC250001"));
	}

}
