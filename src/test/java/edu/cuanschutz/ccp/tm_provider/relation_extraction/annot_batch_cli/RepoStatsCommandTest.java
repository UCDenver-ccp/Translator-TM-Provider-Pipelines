package edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class RepoStatsCommandTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testFindOverlappingBatches() {
		List<String> batchKeys = Arrays.asList("key1", "key2", "key3", "key4");
		Map<String, Set<String>> batchKeyToSentenceIdsMap = new HashMap<String, Set<String>>();
		batchKeyToSentenceIdsMap.put("key1", CollectionsUtil.createSet("sent1", "sent2", "sent3", "sent4", "sent5"));
		batchKeyToSentenceIdsMap.put("key2", CollectionsUtil.createSet("sent1", "sent2", "sent3", "sent4", "sent5"));
		batchKeyToSentenceIdsMap.put("key3", CollectionsUtil.createSet("sent6", "sent7", "sent8", "sent9", "sent10"));
		batchKeyToSentenceIdsMap.put("key4", CollectionsUtil.createSet("sent6", "sent9", "sent11", "sent12", "sent13"));

		int[][] overlappingBatches = RepoStatsCommand.findOverlappingBatches(batchKeyToSentenceIdsMap, batchKeys);

		for (int i = 0; i < 4; i++) {
			for (int j = 0; j < 4; j++) {
				System.out.print(String.format("%" + 3 + "s", overlappingBatches[i][j]));
			}
			System.out.println();
		}

		assertArrayEquals(new int[] { 5, 5, 0, 0 }, overlappingBatches[0]);
		assertArrayEquals(new int[] { 5, 5, 0, 0 }, overlappingBatches[1]);
		assertArrayEquals(new int[] { 0, 0, 5, 2 }, overlappingBatches[2]);
		assertArrayEquals(new int[] { 0, 0, 2, 5 }, overlappingBatches[3]);

	}

}
