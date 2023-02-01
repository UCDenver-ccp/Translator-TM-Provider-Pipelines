package edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli;

import static edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.computeSentenceIdentifier;
import static edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli.ElasticsearchToBratExporterCLI.getAnnotProjectDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * This command interrogates the annotation directory and prints to the console
 * a catalog of the directory contents.
 *
 */
@Command(name = "stats")
public class RepoStatsCommand implements Runnable {

	@Option(names = { "-d", "--dir" }, required = true)
	private File repoBaseDir;

	@Option(names = { "-b", "--biolink" }, required = true)
	private String biolinkAssociation;

	@Override
	public void run() {
		reportStats();
	}

	private void reportStats() {
		try {
			reportOverlapMatrix();
		} catch (IOException e) {
			System.err.println("Overlap matrix reporting failed with error: " + e.getMessage());
			System.exit(-1);
		}
		try {
			reportBatchSizes();
		} catch (IOException e) {
			System.err.println("Batch size reporting failed with error: " + e.getMessage());
			System.exit(-1);
		}

	}

	private void reportBatchSizes() throws IOException {
		File projectDir = getAnnotProjectDir(repoBaseDir, biolinkAssociation);
		if (projectDir == null) {
			System.exit(-1);
		}

		Map<String, Map<String, Integer>> annotatorToBatchCountMap = countBatchSizes(projectDir);
		reportBatchSizeSummary(annotatorToBatchCountMap);
	}

	/**
	 * Prints a summary containing batch sizes for the specified biolink association
	 * 
	 * @param annotatorToBatchCountMap
	 */
	private void reportBatchSizeSummary(Map<String, Map<String, Integer>> annotatorToBatchCountMap) {
		Map<String, Map<String, Integer>> sortedMap = CollectionsUtil.sortMapByKeys(annotatorToBatchCountMap,
				SortOrder.ASCENDING);

		System.out.println(String.format("\n====== Batch summary: %s ======\n", biolinkAssociation));
		for (Entry<String, Map<String, Integer>> entry : sortedMap.entrySet()) {
			reportAnnotatorBatchCounts(entry);
		}
		System.out.println("==============================================================\n");
	}

	/**
	 * For a single annotator, report batch counts by printing them to the console
	 * 
	 * @param entry
	 */
	private void reportAnnotatorBatchCounts(Entry<String, Map<String, Integer>> entry) {
		Map<String, Integer> sortedByBatchName = CollectionsUtil.sortMapByKeys(entry.getValue(), SortOrder.ASCENDING);
		int annotatorTotal = 0;
		String annotatorKey = entry.getKey();
		for (Entry<String, Integer> batchEntry : sortedByBatchName.entrySet()) {
			int count = batchEntry.getValue();
			annotatorTotal += count;
			String batchKey = batchEntry.getKey();
			System.out.println(String.format("%s -- %s: %d", annotatorKey, batchKey, count));
		}
		System.out.println(String.format("%s TOTAL: %d", annotatorKey, annotatorTotal));
		System.out.println("---------------------------\n");
	}

	/**
	 * Traverse the project directory and count sentences in batches. We assume that
	 * any subdirectories in the project directory are annotator-specific, and any
	 * sub-directories of those directories are batch directories
	 * 
	 * @param projectDir
	 * @return
	 * @throws IOException
	 */
	private Map<String, Map<String, Integer>> countBatchSizes(File projectDir) throws IOException {
		Map<String, Map<String, Integer>> annotatorToBatchCountMap = new HashMap<String, Map<String, Integer>>();

		for (Iterator<File> fileIterator = FileUtil.getFileIterator(projectDir, true, ".txt"); fileIterator
				.hasNext();) {
			File file = fileIterator.next();

			String batchKey = file.getParentFile().getName();
			String annotatorKey = file.getParentFile().getParentFile().getName();
			int sentenceCount = countSentences(file);

			updateCountMap(annotatorToBatchCountMap, batchKey, annotatorKey, sentenceCount);
		}

		return annotatorToBatchCountMap;
	}

	/**
	 * Add the specified sentence count to the input map in the appropriate place
	 * 
	 * @param annotatorToBatchCountMap
	 * @param batchKey
	 * @param annotatorKey
	 * @param sentenceCount
	 */
	private void updateCountMap(Map<String, Map<String, Integer>> annotatorToBatchCountMap, String batchKey,
			String annotatorKey, int sentenceCount) {
		Map<String, Integer> map = null;
		if (annotatorToBatchCountMap.containsKey(annotatorKey)) {
			map = annotatorToBatchCountMap.get(annotatorKey);
		} else {
			map = new HashMap<String, Integer>();
			annotatorToBatchCountMap.put(annotatorKey, map);
		}
		CollectionsUtil.addToCountMap(batchKey, sentenceCount, map);
	}

	/**
	 * Given a text file, count the sentences in it. Note that many of the
	 * annotation files contain a line that has only "DONE". We will not count those
	 * lines (or blank lines) as being sentences.
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private int countSentences(File file) throws IOException {
		int lineCount = 0;
		for (StreamLineIterator lineIter = new StreamLineIterator(file, CharacterEncoding.UTF_8); lineIter.hasNext();) {
			Line line = lineIter.next();
			String lineText = line.getText();
			if (!lineText.trim().isEmpty() && !lineText.equals("DONE")) {
				lineCount++;
			}
		}
		return lineCount;
	}

	private void reportOverlapMatrix() throws IOException {
		File projectDir = getAnnotProjectDir(repoBaseDir, biolinkAssociation);
		if (projectDir == null) {
			System.exit(-1);
		}
		Map<String, Set<String>> batchKeyToSentenceIdsMap = linkAnnotatorToSentenceIds(projectDir);
		List<String> batchKeys = new ArrayList<String>(batchKeyToSentenceIdsMap.keySet());
		Collections.sort(batchKeys);
		int[][] batchOverlapCounts = findOverlappingBatches(batchKeyToSentenceIdsMap, batchKeys);
		reportOverlapMatrix(batchOverlapCounts, batchKeys);

	}

	/**
	 * @param batchKeyToSentenceIdsMap
	 * @param keys
	 * @return a matrix indicating the number of overlapping sentences in groups.
	 *         The columns and rows are ordered by the input "keys" list.
	 */
	@VisibleForTesting
	protected static int[][] findOverlappingBatches(Map<String, Set<String>> batchKeyToSentenceIdsMap,
			List<String> batchKeys) {
		int[][] overlaps = new int[batchKeys.size()][batchKeys.size()];

		for (int i = 0; i < batchKeys.size(); i++) {
			for (int j = 0; j < batchKeys.size(); j++) {
				Set<String> iIds = new HashSet<String>(batchKeyToSentenceIdsMap.get(batchKeys.get(i)));
				Set<String> jIds = new HashSet<String>(batchKeyToSentenceIdsMap.get(batchKeys.get(j)));

				iIds.retainAll(jIds);
				int overlap = iIds.size();
				overlaps[i][j] = overlap;
			}
		}

		return overlaps;
	}

	/**
	 * pretty-prints the batch overlap counts matrix
	 * 
	 * @param batchOverlapCounts
	 * @param batchKeys
	 */
	private void reportOverlapMatrix(int[][] batchOverlapCounts, List<String> batchKeys) {

		int spacing = getSpacing(batchKeys);
		int maxKeyLength = getMaxKeyLength(batchKeys);
		printHeader(batchKeys, spacing, maxKeyLength);
		printSeparator(batchKeys.size(), spacing, maxKeyLength);

		for (int i = 0; i < batchKeys.size(); i++) {
			System.out.print(String.format("%" + maxKeyLength + "s", batchKeys.get(i)));
			for (int j = 0; j < batchKeys.size(); j++) {
				System.out.print(String.format("%" + spacing + "s", batchOverlapCounts[i][j]));
			}
			System.out.println("");
		}
	}

	/**
	 * @param batchKeys
	 * @return the length of the longest element in the input list
	 */
	private int getMaxKeyLength(List<String> batchKeys) {
		int max = 0;
		for (String bk : batchKeys) {
			if (max < bk.length()) {
				max = bk.length();
			}
		}
		return max;
	}

	/**
	 * prints dashed line between header and the rest of the table
	 * 
	 * @param count
	 * @param spacing
	 * @param maxKeyLength
	 */
	private void printSeparator(int count, int spacing, int maxKeyLength) {

		StringBuilder firstSep = new StringBuilder();
		for (int i = 0; i < maxKeyLength; i++) {
			firstSep.append("-");
		}
		// create space for the labels on the left side of the table
		System.out.print(String.format("%" + spacing + "s", firstSep.toString()));

		StringBuilder sep = new StringBuilder();
		for (int i = 0; i < spacing - 2; i++) {
			sep.append("-");
		}
		for (int i = 0; i < count; i++) {
			System.out.print(String.format("%" + spacing + "s", sep.toString()));
		}
		System.out.println();
	}

	/**
	 * Finds the longest batch key substring after splitting batch keys at
	 * underscores.
	 * 
	 * @param batchKeys
	 * @return
	 */
	private int getSpacing(List<String> batchKeys) {
		int max = 0;
		for (String bk : batchKeys) {
			String[] toks = bk.split("\\_");
			for (String tok : toks) {
				if (tok.length() + 1 > max) {
					max = tok.length() + 1;
				}
			}
		}
		return max + 2;
	}

	private void printHeader(List<String> batchKeys, int spacing, int maxKeyLength) {
		List<String> remaining = new ArrayList<String>(batchKeys);
		while (notEmpty(remaining)) {
			// create space for the labels on the left side of the table
			System.out.print(String.format("%" + maxKeyLength + "s", ""));
			for (int i = 0; i < remaining.size(); i++) {
				String bk = remaining.get(i);
				int underscoreIndex = bk.indexOf("_");
				String keyPart = null;
				if (underscoreIndex > 0) {
					keyPart = bk.substring(0, underscoreIndex + 1);
					remaining.set(i, bk.substring(underscoreIndex + 1));
				} else {
					keyPart = bk;
					remaining.set(i, "");
				}
				System.out.print(String.format("%" + spacing + "s", keyPart));
			}
			System.out.println();
		}
	}

	/**
	 * @param remaining
	 * @return true if any of the list elements are non-empty, false otherwise
	 */
	private boolean notEmpty(List<String> list) {
		for (String s : list) {
			if (!s.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates a map that links annotators to their batches and the sentence
	 * identifiers in each batch
	 * 
	 * @param projectDir
	 * @return
	 * @throws IOException
	 */
	private Map<String, Set<String>> linkAnnotatorToSentenceIds(File projectDir) throws IOException {
		Map<String, Set<String>> batchKeyToSentenceIdsMap = new HashMap<String, Set<String>>();

		for (Iterator<File> fileIterator = FileUtil.getFileIterator(projectDir, true, ".txt"); fileIterator
				.hasNext();) {
			File file = fileIterator.next();

			String batchKey = file.getParentFile().getName();
			String annotatorKey = file.getParentFile().getParentFile().getName();
			String key = String.format("%s_%s", annotatorKey, batchKey);
			Set<String> sentenceIds = getSentenceIds(file);
			for (String id : sentenceIds) {
				CollectionsUtil.addToOne2ManyUniqueMap(key, id, batchKeyToSentenceIdsMap);
			}
		}

		return batchKeyToSentenceIdsMap;
	}

	/**
	 * Computes a hash of the sentence text to use as an identifier. This is the
	 * same hash used when storing the sentence in Elasticsearch.
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	protected static Set<String> getSentenceIds(File file) throws IOException {
		Set<String> ids = new HashSet<String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(file, CharacterEncoding.UTF_8); lineIter.hasNext();) {
			Line line = lineIter.next();
			String lineText = line.getText();
			if (!lineText.trim().isEmpty() && !lineText.equals("DONE")) {
				String id = computeSentenceIdentifier(lineText);
				ids.add(id);
			}
		}
		return ids;
	}

//	/**
//	 * Updates the specified map by storing the sentence ids associated with the
//	 * approprate annotator and batch
//	 * 
//	 * @param batchToSentenceIdsMap
//	 * @param batchKey
//	 * @param annotatorKey
//	 * @param sentenceIds
//	 */
//	private void updateIdsMap(Map<String, Set<String>> batchToSentenceIdsMap, String batchKey,
//			Set<String> sentenceIds) {
//
//		Map<String, Set<String>> map = null;
//		if (batchToSentenceIdsMap.containsKey(batchKey)) {
//			map = batchToSentenceIdsMap.get(batchKey);
//		} else {
//			map = new HashMap<String, Set<String>>();
//			batchToSentenceIdsMap.put(annotatorKey, map);
//		}
//		for (String id : sentenceIds) {
//			CollectionsUtil.addToOne2ManyUniqueMap(batchKey, id, map);
//		}
//	}

}
