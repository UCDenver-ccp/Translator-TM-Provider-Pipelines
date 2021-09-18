package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;

public class ConceptCooccurrenceCountsPipelineTest {

	@Test
	public void testGetLevelToConceptCountFileNamePrefix() {
		String collection = "PUBMED_SUB_31";
		String filenamePrefix = ConceptCooccurrenceCountsPipeline
				.getLevelToConceptCountFileNamePrefix(CooccurLevel.SENTENCE, collection);

		String expectedFilenamePrefix = "sentence"
				+ ConceptCooccurrenceCountsPipeline.LEVEL_TO_CONCEPT_COUNT_FILE_PREFIX + "." + collection;

		System.out.println(filenamePrefix);
		assertEquals(expectedFilenamePrefix, filenamePrefix);
	}

	@Test
	public void testGetConceptPairToLevelFileNamePrefix() {
		String collection = "PUBMED_SUB_31";
		String filenamePrefix = ConceptCooccurrenceCountsPipeline
				.getConceptPairToLevelFileNamePrefix(CooccurLevel.SENTENCE, collection);

		String expectedFilenamePrefix = ConceptCooccurrenceCountsPipeline.CONCEPT_PAIR_TO_DOC_FILE_PREFIX + "sentence."
				+ collection;

		System.out.println(filenamePrefix);
		assertEquals(expectedFilenamePrefix, filenamePrefix);
	}

	@Test
	public void testGetConceptIdToLevelFileNamePrefix() {
		String collection = "PUBMED_SUB_31";
		String filenamePrefix = ConceptCooccurrenceCountsPipeline
				.getConceptIdToLevelFileNamePrefix(CooccurLevel.SENTENCE, collection);

		String expectedFilenamePrefix = ConceptCooccurrenceCountsPipeline.CONCEPT_ID_TO_DOC_FILE_PREFIX + "sentence."
				+ collection;

		System.out.println(filenamePrefix);
		assertEquals(expectedFilenamePrefix, filenamePrefix);
	}

}
