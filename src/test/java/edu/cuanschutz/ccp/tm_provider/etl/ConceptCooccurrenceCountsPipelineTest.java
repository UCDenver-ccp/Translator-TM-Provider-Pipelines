package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;

public class ConceptCooccurrenceCountsPipelineTest {

	@Test
	public void testGetLevelToConceptCountFileNamePrefix() {
		String collection = "PUBMED_SUB_31";
		String filenamePrefix = ConceptCooccurrenceCountsPipeline
				.getDocumentIdToConceptIdsFileNamePrefix(CooccurLevel.SENTENCE, collection);

		String expectedFilenamePrefix = "sentence"
				+ ConceptCooccurrenceCountsPipeline.DOCUMENT_ID_TO_CONCEPT_ID_FILE_PREFIX + "." + collection;

		System.out.println(filenamePrefix);
		assertEquals(expectedFilenamePrefix, filenamePrefix);
	}

}
