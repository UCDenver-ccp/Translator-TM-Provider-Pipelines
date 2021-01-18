package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class SentenceExtractionPipelineTest {

	@Test
	public void testCompileKeywords() {
		assertEquals(new HashSet<String>(), SentenceExtractionPipeline.compileKeywords(null));
		assertEquals(new HashSet<String>(), SentenceExtractionPipeline.compileKeywords(""));
		assertEquals(CollectionsUtil.createSet("word1", "word2", "word3"),
				SentenceExtractionPipeline.compileKeywords("word1|word2|word3"));

	}

}
