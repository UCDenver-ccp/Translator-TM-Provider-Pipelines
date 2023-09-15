package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Data;

public class SemmedDbFebRelayResultsCompiler {

	private static final String ASSERTION_CORRECT = "Assertion Correct";
	private static final String ASSERTION_INCORRECT = "Assertion Incorrect";

	private static final Set<String> CHEMICAL_TYPES = new HashSet<String>(
			Arrays.asList("aapp", "antb", "bacs", "bodm", "chem", "chvf", "chvs", "clnd", "elii", "enzy", "hops",
					"horm", "imft", "irda", "inch", "nnon", "orch", "phsu", "rcpt", "vita"));
	private static final Set<String> TREATMENT_TYPES = new HashSet<String>(
			Arrays.asList("diap", "edac", "hlca", "lbpr", "mbrt", "resa", "topp", "drdd", "medd", "resd"));

	public static void main(String[] args) {
		File resultsFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_eval_results.csv");
		File metadataFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_treats_sample_5000.csv");
		try {
			computeOverallSemmedAccuracy(resultsFile);

			computeSemmedAccuracyBySubjectType(resultsFile, metadataFile);
			computeSemmedAccuracyBySentenceComplexity(resultsFile, metadataFile);
			computeIncorrectReasonDistribution(resultsFile);
			computeImprovementDistribution(resultsFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * <pre>
	 *  
	 * feedback_id 
	 * predication_id
	 * answer_id
	 * answer
	 * response
	 * </pre>
	 * 
	 * @throws IOException
	 */

	public static void computeOverallSemmedAccuracy(File resultsFile) throws IOException {

		Set<String> feedbackIds = new HashSet<String>();
		Map<String, String> predicationIdToAnswer = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			feedbackIds.add(feedbackId);

			/*
			 * map from predication ID to a result. If any result is "incorrect" then we
			 * will assign incorrect to that predication id
			 */

			if (answer.equals(ASSERTION_INCORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}

			if (!predicationIdToAnswer.containsKey(predicationId) && answer.equals(ASSERTION_CORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}

		}

		System.out.println("# reviewed assertions: " + feedbackIds.size());
		System.out.println("# unique reviewed assertions: " + predicationIdToAnswer.size());

		int correctCount = 0;
		int incorrectCount = 0;
		for (Entry<String, String> entry : predicationIdToAnswer.entrySet()) {
			if (entry.getValue().equals(ASSERTION_CORRECT)) {
				correctCount++;
			} else if (entry.getValue().equals(ASSERTION_INCORRECT)) {
				incorrectCount++;
			} else {
				throw new IllegalStateException("should only be correct or incorrect");
			}
		}

		System.out.println("Correct count: " + correctCount);
		System.out.println("Incorrect count: " + incorrectCount);
		int total = correctCount + incorrectCount;
		System.out.println("Total: " + total);
		System.out.println("Accuracy: " + ((float) correctCount / (float) total));

	}

	public static void computeIncorrectReasonDistribution(File resultsFile) throws IOException {

		Set<String> feedbackIds = new HashSet<String>();
		Map<String, String> incorrectPredicationIdToFeedbackId = new HashMap<String, String>();

		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			feedbackIds.add(feedbackId);

			/*
			 * map from predication ID to a result. If any result is "incorrect" then we
			 * will assign incorrect to that predication id
			 */

			if (answer.equals(ASSERTION_INCORRECT)) {
				incorrectPredicationIdToFeedbackId.put(predicationId, feedbackId);
			}
		}

		System.out.println("\n");
		System.out.println("Size incorrectPredicationIdToFeedbackId: " + incorrectPredicationIdToFeedbackId.size());

		Map<String, Integer> incorrectReasonToCountMap = new HashMap<String, Integer>();

		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			if (incorrectPredicationIdToFeedbackId.values().contains(feedbackId)) {
				CollectionsUtil.addToCountMap(answer, incorrectReasonToCountMap);
			}
		}

		for (Entry<String, Integer> entry : incorrectReasonToCountMap.entrySet()) {
			System.out.println("REASON: " + entry.getKey() + " -- " + entry.getValue() + " -- "
					+ ((float) entry.getValue() / incorrectPredicationIdToFeedbackId.size()));
		}

	}

	public static void computeImprovementDistribution(File resultsFile) throws IOException {

		Set<String> feedbackIds = new HashSet<String>();
		Map<String, String> predicationToAnswerId = new HashMap<String, String>();
		Map<String, String> correctPredicationIdToFeedbackId = new HashMap<String, String>();

		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			feedbackIds.add(feedbackId);

			/*
			 * map from predication ID to a result. If any result is "incorrect" then we
			 * will assign incorrect to that predication id
			 */

			if (answer.equals(ASSERTION_INCORRECT)) {
				predicationToAnswerId.put(predicationId, answer);
			}

			if (!predicationToAnswerId.containsKey(predicationId) && answer.equals(ASSERTION_CORRECT)) {
				predicationToAnswerId.put(predicationId, answer);
			}
		}
		Map<String, Integer> improvementsToCountMap = new HashMap<String, Integer>();
		Map<String, String> predicationIdToFeedbackIdMap = new HashMap<String, String>();
		Set<String> alreadyDonePredicationIds = new HashSet<String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			try {
				if (predicationToAnswerId.get(predicationId).equals(ASSERTION_CORRECT)) {
					predicationIdToFeedbackIdMap.put(predicationId, feedbackId);
				}
			} catch (Exception e) {

			}

		}

		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			if (predicationIdToFeedbackIdMap.containsKey(predicationId)
					&& predicationIdToFeedbackIdMap.get(predicationId).equals(feedbackId)) {
				CollectionsUtil.addToCountMap(answer, improvementsToCountMap);
			}

		}

		System.out.println();
		int total = improvementsToCountMap.get(ASSERTION_CORRECT);
		for (Entry<String, Integer> entry : improvementsToCountMap.entrySet()) {
			System.out.println("IMPROVEMENT: " + entry.getKey() + " -- " + entry.getValue() + " -- "
					+ (float) entry.getValue() / (float) total);
		}

	}

	public static void computeSemmedAccuracyBySubjectType(File resultsFile, File metadataFile) throws IOException {

		Map<String, Metadata> predicationIdToMetadataMap = getPredicationIdToMetadataMap(metadataFile);
		Set<String> feedbackIds = new HashSet<String>();
		Map<String, String> predicationIdToAnswer = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			feedbackIds.add(feedbackId);

			/*
			 * map from predication ID to a result. If any result is "incorrect" then we
			 * will assign incorrect to that predication id
			 */

			if (answer.equals(ASSERTION_INCORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}
			if (!predicationIdToAnswer.containsKey(predicationId) && answer.equals(ASSERTION_CORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}

		}

		System.out.println("\n");
		System.out.println("# reviewed assertions: " + feedbackIds.size());
		System.out.println("# unique reviewed assertions: " + predicationIdToAnswer.size());

		int chemicalCorrectCount = 0;
		int chemicalIncorrectCount = 0;
		int treatmentCorrectCount = 0;
		int treatmentIncorrectCount = 0;

		for (Entry<String, String> entry : predicationIdToAnswer.entrySet()) {
			String predicationId = entry.getKey();

			String subjectType = null;
			if (predicationIdToMetadataMap.containsKey(predicationId)) {
				subjectType = predicationIdToMetadataMap.get(predicationId).getSubjectType();
			} else {
				System.err.println("Missing: predicationId: " + predicationId);
				continue;
			}

			if (entry.getValue().equals(ASSERTION_CORRECT)) {
				if (CHEMICAL_TYPES.contains(subjectType)) {
					chemicalCorrectCount++;
				} else if (TREATMENT_TYPES.contains(subjectType)) {
					treatmentCorrectCount++;
				} else {
					throw new IllegalStateException();
				}
			} else if (entry.getValue().equals(ASSERTION_INCORRECT)) {
				if (CHEMICAL_TYPES.contains(subjectType)) {
					chemicalIncorrectCount++;
				} else if (TREATMENT_TYPES.contains(subjectType)) {
					treatmentIncorrectCount++;
				} else {
					throw new IllegalStateException();
				}
			} else {
				throw new IllegalStateException("should only be correct or incorrect");
			}
		}

		{
			System.out.println("CHEMICAL Correct count: " + chemicalCorrectCount);
			System.out.println("CHEMICAL Incorrect count: " + chemicalIncorrectCount);
			int chemicalTotal = chemicalCorrectCount + chemicalIncorrectCount;
			System.out.println("CHEMICAL Total: " + chemicalTotal);
			System.out.println("CHEMICAL Accuracy: " + ((float) chemicalCorrectCount / (float) chemicalTotal));
		}

		{
			System.out.println("\n");
			System.out.println("TREATMENT Correct count: " + treatmentCorrectCount);
			System.out.println("TREATMENT Incorrect count: " + treatmentIncorrectCount);
			int treatmentTotal = treatmentCorrectCount + treatmentIncorrectCount;
			System.out.println("TREATMENT Total: " + treatmentTotal);
			System.out.println("TREATMENT Accuracy: " + ((float) treatmentCorrectCount / (float) treatmentTotal));
		}

	}

	public static void computeSemmedAccuracyBySentenceComplexity(File resultsFile, File metadataFile)
			throws IOException {

		Map<String, Metadata> predicationIdToMetadataMap = getPredicationIdToMetadataMap(metadataFile);
		Set<String> feedbackIds = new HashSet<String>();
		Map<String, String> predicationIdToAnswer = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(resultsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %s\n", line.getText()));
				e.printStackTrace();
				System.exit(-1);
			}

			String feedbackId = nextRecord[0];
			String predicationId = nextRecord[1];
			String answerId = nextRecord[2];
			String answer = nextRecord[3];
			String response = nextRecord[4];

			// ignore test records
			if (predicationId.equals("10595361") || predicationId.equals("10612762")) {
				continue;
			}

			feedbackIds.add(feedbackId);

			/*
			 * map from predication ID to a result. If any result is "incorrect" then we
			 * will assign incorrect to that predication id
			 */

			if (answer.equals(ASSERTION_INCORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}
			if (!predicationIdToAnswer.containsKey(predicationId) && answer.equals(ASSERTION_CORRECT)) {
				predicationIdToAnswer.put(predicationId, answer);
			}

		}

		System.out.println("\n");
		System.out.println("# reviewed assertions: " + feedbackIds.size());
		System.out.println("# unique reviewed assertions: " + predicationIdToAnswer.size());

		int lowComplexityCorrectCount = 0;
		int lowComplexityIncorrectCount = 0;
		int highComplexityCorrectCount = 0;
		int highComplexityIncorrectCount = 0;

		for (Entry<String, String> entry : predicationIdToAnswer.entrySet()) {
			String predicationId = entry.getKey();

			boolean lowComplexity = true;
			if (predicationIdToMetadataMap.containsKey(predicationId)) {
				int interveningTokenCount = predicationIdToMetadataMap.get(predicationId).getInterveningTokenCount();
				if (interveningTokenCount > 10) {
					lowComplexity = false;
				}
			} else {
				System.err.println("Missing: predicationId: " + predicationId);
				continue;
			}

			if (entry.getValue().equals(ASSERTION_CORRECT)) {
				if (lowComplexity) {
					lowComplexityCorrectCount++;
				} else {
					highComplexityCorrectCount++;
				}
			} else if (entry.getValue().equals(ASSERTION_INCORRECT)) {
				if (lowComplexity) {
					lowComplexityIncorrectCount++;
				} else {
					highComplexityIncorrectCount++;
				}
			} else {
				throw new IllegalStateException("should only be correct or incorrect");
			}
		}

		{
			System.out.println("LOW COMPLEXITY Correct count: " + lowComplexityCorrectCount);
			System.out.println("LOW COMPLEXITY Incorrect count: " + lowComplexityIncorrectCount);
			int lowComplexityTotal = lowComplexityCorrectCount + lowComplexityIncorrectCount;
			System.out.println("LOW COMPLEXITY Total: " + lowComplexityTotal);
			System.out.println(
					"LOW COMPLEXITY Accuracy: " + ((float) lowComplexityCorrectCount / (float) lowComplexityTotal));
		}

		{
			System.out.println("\n");
			System.out.println("HIGH COMPLEXITY Correct count: " + highComplexityCorrectCount);
			System.out.println("HIGH COMPLEXITY Incorrect count: " + highComplexityIncorrectCount);
			int highComplexityTotal = highComplexityCorrectCount + highComplexityIncorrectCount;
			System.out.println("HIGH COMPLEXITY Total: " + highComplexityTotal);
			System.out.println(
					"HIGH COMPLEXITY Accuracy: " + ((float) highComplexityCorrectCount / (float) highComplexityTotal));
		}

	}

	private static Map<String, Metadata> getPredicationIdToMetadataMap(File metadataFile) throws IOException {
		Map<String, Metadata> map = new HashMap<String, Metadata>();
		for (StreamLineIterator lineIter = new StreamLineIterator(metadataFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() == 0) {
				// skip header
				continue;
			}

			String[] cols = line.getText().split(",");

			String predicationId = cols[0];
			int interveningTokenCount = Integer.parseInt(cols[1]);
			int sentenceTokenCount = Integer.parseInt(cols[2]);
			int pmidCount = Integer.parseInt(cols[3]);
			String[] subjObj = cols[4].split("_");
			String subType = subjObj[0];
			String objType = subjObj[1];

			Metadata m = new Metadata(predicationId, interveningTokenCount, sentenceTokenCount, pmidCount, subType,
					objType);

			map.put(predicationId, m);
		}

		return map;
	}

	@Data
	private static class Metadata {
		private final String predicationId;
		private final int interveningTokenCount;
		private final int sentenceTokenCount;
		private final int pmidCount;
		private final String subjectType;
		private final String objectType;
	}

	public static void computeIaa(File resultsFile) {

	}

}
