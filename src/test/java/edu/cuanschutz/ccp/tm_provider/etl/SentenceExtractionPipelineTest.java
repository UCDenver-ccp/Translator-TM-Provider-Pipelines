package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

	@Test
	public void testBuildPrefixToPlaceholderMap() {

		String prefixesX = "PR";
		String placeholderX = "$GENE";
		String prefixesY = "UBERON|CL|GO:0005575";
		String placeholderY = "$LOCATION$";

		Map<List<String>, String> prefixToPlaceholderMap = SentenceExtractionPipeline
				.buildPrefixToPlaceholderMap(prefixesX, placeholderX, prefixesY, placeholderY);

		Map<List<String>, String> expectedPrefixToPlaceholderMap = new HashMap<List<String>, String>();
		expectedPrefixToPlaceholderMap.put(Arrays.asList("CL", "GO:0005575", "UBERON"), placeholderY);
		expectedPrefixToPlaceholderMap.put(Arrays.asList("PR"), placeholderX);

		assertEquals(expectedPrefixToPlaceholderMap, prefixToPlaceholderMap);

	}

}
