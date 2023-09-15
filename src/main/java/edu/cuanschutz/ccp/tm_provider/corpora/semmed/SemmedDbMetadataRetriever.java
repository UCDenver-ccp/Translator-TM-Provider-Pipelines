package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import lombok.Data;

/**
 * This class creates an updated metadata file that will be combined with the
 * evaluation results obtained during the Feb 2023 relay. This class parses a
 * file with metadata for the 5000 predications that were selected for the relay
 * session.
 * 
 * This class also creates a file with sentences+placeholders to use as BERT
 * input for running our treats/causes model over these Semmed predications.
 *
 */
public class SemmedDbMetadataRetriever {

	public static void main(String[] args) {
		// input files
		File existingMetadataFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_treats_sample_5000.csv");
		File sampleSentencesFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_sample_sentences.csv.gz");

		// only available after processing the bert input file created here with the
		// bert model
		File bertOutputFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/bl_chemical_to_disease_or_phenotypic_feature.0.4.semmed_sentences.classified_sentences.tsv");
		// only available after running the node norm query created here
		File nodeNormCuiQueryOutput = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/node_norm_cui_output.json");
		// UMLS MRCONSO.RRF file
		File umlsMrconsoFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/MRCONSO.ENG.RRF.gz");
		// only available after computing CUI IDF below
		File cui2idfFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/cui2idf.csv.gz");

		// output files
		File updatedMetadataFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_treats_sample_5000.metadata.csv");
		File bertInputFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_treats_sample_5000.bert.tsv");
		File cuisFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_treats_sample.cuis.tsv");
		File nodeNormCuiQueryFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/query_node_norm_for_cuis.sh");
		File outputDir = new File("/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results");

		try {
			Map<String, Metadata> predicationIdToMetadataMap = compileMetadata(sampleSentencesFile);
			Map<String, Boolean> predicationIdToIsTreatsMap = parseBertOutputFile(bertOutputFile);
			Map<String, Float> predicationIdToNodeNormInformationContent = getCuiToNodeNormInformationContentMap(
					nodeNormCuiQueryOutput);
			Map<String, Float> cuiToIdfMap = loadCuiToIdfMap(cui2idfFile);
			writeUpdatedMetadataFile(existingMetadataFile, updatedMetadataFile, predicationIdToMetadataMap,
					predicationIdToIsTreatsMap, predicationIdToNodeNormInformationContent, cuiToIdfMap);

			Set<String> cuis = writeCuiListFile(predicationIdToMetadataMap.values(), cuisFile);

			writeNodeNormCuiQuery(cuis, nodeNormCuiQueryFile);
			writeBertInputFile(predicationIdToMetadataMap, bertInputFile);
//			writeCuiToMeshFile(predicationIdToMetadataMap.values(), umlsMrconsoFile, outputDir, "SNOMEDCT_US",
//					"snomedct");
//			writeCuiToMeshFile(predicationIdToMetadataMap.values(), umlsMrconsoFile, outputDir, "MSH", "mesh");
//			writeCuiToMeshFile(predicationIdToMetadataMap.values(), umlsMrconsoFile, outputDir, "HPO", "hpo");
//			writeCuiToMeshFile(predicationIdToMetadataMap.values(), umlsMrconsoFile, outputDir, "NCI", "nci");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Loads a mapping from CUI to IDF from the specified file
	 * 
	 * @param cui2idfFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Map<String, Float> loadCuiToIdfMap(File cui2idfFile) throws FileNotFoundException, IOException {
		Map<String, Float> cuiToIdfMap = new HashMap<String, Float>();
		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(cui2idfFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 1000 == 0) {
				System.out.println("progress: " + line.getLineNumber());
			}

			// skip the header
			if (line.getLineNumber() == 0) {
				continue;
			}

			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %d\n", line.getLineNumber()));
				continue;
			}
			String cui = nextRecord[0];
			String name = nextRecord[1];
			Float idf = Float.parseFloat(nextRecord[2]);
			cuiToIdfMap.put(cui, idf);
		}
		return cuiToIdfMap;
	}

	/**
	 * Find mappings to MeSH for each CUI using the UMLS MRCONSO file
	 * 
	 * @param cuis
	 * @param umlsMrconsoFile
	 * @param cui2meshFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static void writeCuiToMeshFile(Collection<Metadata> metadata, File umlsMrconsoFile, File outputDir,
			String sourceAbbrev, String sourceLabel) throws FileNotFoundException, IOException {

		File subjectCui2meshFile = new File(outputDir, String.format("subjectCui2%s.tsv", sourceLabel));
		File objectCui2meshFile = new File(outputDir, String.format("objectCui2%s.tsv", sourceLabel));
		File subjectCuiMissingMeshFile = new File(outputDir, String.format("subjectCuiMissing_%s.tsv", sourceLabel));
		File objectCuiMissingMeshFile = new File(outputDir, String.format("objectCuiMissing_%s.tsv", sourceLabel));

		System.out.println("Writing cui2X mapping file...");
		Map<String, String> subjectCuiToMeshMap = new HashMap<String, String>();
		Map<String, String> objectCuiToMeshMap = new HashMap<String, String>();
		Set<String> observedCuis = new HashSet<String>();

		Set<String> subjectCuis = new HashSet<String>();
		Set<String> objectCuis = new HashSet<String>();
		for (Metadata md : metadata) {
			subjectCuis.add(md.getSubjectCui());
			objectCuis.add(md.getObjectCui());
		}

		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(umlsMrconsoFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("Progress: %d (%d of %d)", line.getLineNumber(), observedCuis.size(),
						subjectCuis.size() + objectCuis.size()));
			}

			String[] cols = line.getText().split("\\|");
			String cui = cols[0];
			if (subjectCuis.contains(cui) || objectCuis.contains(cui)) {
				observedCuis.add(cui);
				String sourceName = cols[11];
				if (sourceName.equals(sourceAbbrev)) {
					String sourceCode = cols[13];
					if (subjectCuis.contains(cui)) {
						subjectCuiToMeshMap.put(cui, sourceCode);
					} else {
						objectCuiToMeshMap.put(cui, sourceCode);
					}
				}
			}
			if (observedCuis.size() == (subjectCuis.size() + objectCuis.size())) {
				// then we've seen all CUIs in the MRCONSO file so we can stop
				break;
			}
		}

		// write the cui2mesh mappings
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(subjectCui2meshFile)) {
			for (Entry<String, String> entry : subjectCuiToMeshMap.entrySet()) {
				writer.write(String.format("%s\t%s", entry.getKey(), entry.getValue()));
			}
		}
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(objectCui2meshFile)) {
			for (Entry<String, String> entry : objectCuiToMeshMap.entrySet()) {
				writer.write(String.format("%s\t%s", entry.getKey(), entry.getValue()));
			}
		}

		System.out.println(String.format("Found %s mappings for %d of %d subject CUIs.", sourceLabel,
				subjectCuiToMeshMap.size(), subjectCuis.size()));
		System.out.println(String.format("Found %s mappings for %d of %d object CUIs.", sourceLabel,
				objectCuiToMeshMap.size(), objectCuis.size()));

		// create a file listing cuis that don't have mesh mappings
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(subjectCuiMissingMeshFile)) {
			Set<String> cuisMissingMesh = new HashSet<String>(subjectCuis);
			cuisMissingMesh.removeAll(subjectCuiToMeshMap.keySet());
			for (String cui : cuisMissingMesh) {
				writer.write(cui + "\n");
			}
		}

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(objectCuiMissingMeshFile)) {
			Set<String> cuisMissingMesh = new HashSet<String>(objectCuis);
			cuisMissingMesh.removeAll(objectCuiToMeshMap.keySet());
			for (String cui : cuisMissingMesh) {
				writer.write(cui + "\n");
			}
		}
	}

	private static Map<String, Float> getCuiToNodeNormInformationContentMap(File nodeNormQueryOutputFile)
			throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		Map<String, Float> map = new HashMap<String, Float>();
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Clz>>() {
		}.getType();

		Map<String, Clz> idToClzMap = gson.fromJson(new FileReader(nodeNormQueryOutputFile), type);

		for (Entry<String, Clz> entry : idToClzMap.entrySet()) {
			String cui = entry.getKey();
			Clz clz = entry.getValue();
			if (clz != null) {
				Float ic = entry.getValue().getInformation_content();
				// remove the UMLS: prefix from the CUI
				String cuiNoPrefix = StringUtil.removePrefix(cui, "UMLS:");
				if (ic != null) {
					map.put(cuiNoPrefix, ic);
				} else {
					map.put(cuiNoPrefix, -1f);
				}
			}

		}

		return map;
	}

	@Data
	private static class Clz {
		private IdLabel id;
		private IdLabel[] equivalent_identifiers;
		private String[] type;
		private Float information_content;
	}

	@Data
	private static class IdLabel {
		private String identifier;
		private String label;
	}

	/**
	 * writes a curl command to file that will query the SRI node normalizer for the
	 * input CUIs
	 * 
	 * @param cuis
	 * @param nodeNormCuiQueryFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static void writeNodeNormCuiQuery(Set<String> cuis, File nodeNormCuiQueryFile)
			throws FileNotFoundException, IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("curl -X 'POST' \\\n" + "  'https://nodenorm.transltr.io/1.3/get_normalized_nodes' \\\n"
				+ "  -H 'accept: application/json' \\\n" + "  -H 'Content-Type: application/json' \\\n" + "  -d '{\n"
				+ "  \"curies\": [\n");

		List<String> cuiList = new ArrayList<String>(cuis);
		for (int i = 0; i < cuiList.size(); i++) {
			sb.append(String.format("\"UMLS:%s\"", cuiList.get(i)));
			if (i < cuiList.size() - 1) {
				sb.append(",\n");
			} else {
				sb.append("\n");
			}
		}
		sb.append("  ],\n" + "  \"conflate\": true\n" + "}'");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(nodeNormCuiQueryFile)) {
			writer.write(sb.toString());
		}

	}

	/**
	 * Writes to file all CUIs used as subject/object entities
	 * 
	 * @param metadata
	 * @param cuisFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Set<String> writeCuiListFile(Collection<Metadata> metadata, File cuisFile)
			throws FileNotFoundException, IOException {
		Set<String> cuis = new HashSet<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(cuisFile)) {
			for (Metadata md : metadata) {
				if (!cuis.contains(md.getSubjectCui())) {
					cuis.add(md.getSubjectCui());
					writer.write(md.getSubjectCui() + "\n");
				}
				if (!cuis.contains(md.getObjectCui())) {
					cuis.add(md.getObjectCui());
					writer.write(md.getObjectCui() + "\n");
				}
			}
		}
		return cuis;
	}

	/**
	 * parses the output of running our BERT model on the SemMed sentences; returns
	 * a map from predication ID to True if it's a TREATS relation, false otherwise.
	 * 
	 * @param bertOutputFile
	 * @return
	 * @throws IOException
	 */
	private static Map<String, Boolean> parseBertOutputFile(File bertOutputFile) throws IOException {
		Map<String, Boolean> map = new HashMap<String, Boolean>();
		for (StreamLineIterator lineIter = new StreamLineIterator(bertOutputFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() < 2) {
				// skip first two lines
				continue;
			}

			String[] cols = line.getText().split("\\t");
			String predicationId = cols[0];
			float treatsScore = Float.parseFloat(cols[2]);
			boolean isTreats = (treatsScore > 0.9f);
			map.put(predicationId, isTreats);
		}
		return map;
	}

	private static final String SUBJECT_PLACEHOLDER = "@CHEMICAL$";
	private static final String OBJECT_PLACEHOLDER = "@DISEASE$";

	private static void writeBertInputFile(Map<String, Metadata> predicationIdToMetadataMap, File bertInputFile)
			throws FileNotFoundException, IOException {

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(bertInputFile)) {
			writer.write("ID\tsentence\n");
			for (Metadata md : predicationIdToMetadataMap.values()) {
				String sentenceWithPlaceholders = getSentenceWithPlaceholders(md);
				writer.write(String.format("%s\t%s\n", md.getPredicationId(), sentenceWithPlaceholders));
			}
		}

	}

	private static String getSentenceWithPlaceholders(Metadata md) {
		String sentenceWithPlaceholders = md.getSentence();

		// replace sentence text with placeholders starting from the end of the sentence
		// and working forwards -- this keeps the entity spans unchanged
		if (md.getSubjectStartIndex() > md.getObjectStartIndex()) {
			// then the subject appears after the object in the sentence so we will replace
			// the subject first, then the object.
			sentenceWithPlaceholders = replaceSubject(sentenceWithPlaceholders, md);
			sentenceWithPlaceholders = replaceObject(sentenceWithPlaceholders, md);
		} else {
			sentenceWithPlaceholders = replaceObject(sentenceWithPlaceholders, md);
			sentenceWithPlaceholders = replaceSubject(sentenceWithPlaceholders, md);
		}
		return sentenceWithPlaceholders;
	}

	private static String replaceObject(String sentenceWithPlaceholders, Metadata md) {
		// get indexes relative to the sentence
		int offset = md.getSentenceStartIndex();
		int entityStartIndex = md.getObjectStartIndex() - offset;
		int entityEndIndex = md.getObjectEndIndex() - offset;
		String placeholder = OBJECT_PLACEHOLDER;
		String expectedEntityText = md.getObjectText();

		return replaceEntityTextWithPlaceholder(sentenceWithPlaceholders, entityStartIndex, entityEndIndex, placeholder,
				expectedEntityText);
	}

	private static String replaceSubject(String sentenceWithPlaceholders, Metadata md) {
		// get indexes relative to the sentence
		int offset = md.getSentenceStartIndex();
		int entityStartIndex = md.getSubjectStartIndex() - offset;
		int entityEndIndex = md.getSubjectEndIndex() - offset;
		String placeholder = SUBJECT_PLACEHOLDER;
		String expectedEntityText = md.getSubjectText();

		return replaceEntityTextWithPlaceholder(sentenceWithPlaceholders, entityStartIndex, entityEndIndex, placeholder,
				expectedEntityText);
	}

	private static String replaceEntityTextWithPlaceholder(String sentenceWithPlaceholders, int entityStartIndex,
			int entityEndIndex, String placeholder, String expectedEntityText) {
		// check that the covered text matches the expected entity text
		String coveredText = sentenceWithPlaceholders.substring(entityStartIndex, entityEndIndex);
		if (!coveredText.contentEquals(expectedEntityText)) {
			throw new IllegalArgumentException(String.format(
					"Expected entity text does not match observed: '%s' != '%s'", coveredText, expectedEntityText));
		}

		// replace the entity text with the placeholder
		return sentenceWithPlaceholders.substring(0, entityStartIndex) + placeholder
				+ sentenceWithPlaceholders.substring(entityEndIndex);
	}

	/**
	 * <pre>
	 *  0: SENTENCE_ID </br> 
	 *  1: PMID </br> 
	 *  2: TYPE </br> 
	 *  3: NUMBER </br> 
	 *  4: SENT_START_INDEX </br> 
	 *  5: SENTENCE </br>
	 *  6: SENT_END_INDEX </br> 
	 *  7: SECTION_HEADER </br> 
	 *  8: NORMALIZED_SECTION_HEADER </br> 
	 *  9: BTE_ID </br>
	 * 10: PREDICATION_ID </br> 
	 * 11: SENTENCE_ID </br> 
	 * 12: PMID </br> 
	 * 13: PREDICATE </br> 
	 * 14: SUBJECT_CUI </br> 
	 * 15: SUBJECT_NAME </br> 
	 * 16: SUBJECT_SEMTYPE </br> 
	 * 17: SUBJECT_NOVELTY </br> 
	 * 18: OBJECT_CUI </br> 
	 * 19: OBJECT_NAME </br>
	 * 20: OBJECT_SEMTYPE </br> 
	 * 21: OBJECT_NOVELTY </br> 
	 * 22: SUBJECT_PREFIX </br> 
	 * 23: OBJECT_PREFIX </br>
	 * 24: PREDICATION_AUX_ID </br> 
	 * 25: PREDICATION_ID </br> 
	 * 26: SUBJECT_TEXT </br> 
	 * 27: SUBJECT_DIST </br>
	 * 28: SUBJECT_MAXDIST </br> 
	 * 29: SUBJECT_START_INDEX </br> 
	 * 30: SUBJECT_END_INDEX </br> 
	 * 31: SUBJECT_SCORE </br>
	 * 32: INDICATOR_TYPE </br> 
	 * 33: PREDICATE_START_INDEX </br> 
	 * 34: PREDICATE_END_INDEX </br> 
	 * 35: OBJECT_TEXT </br>
	 * 36: OBJECT_DIST </br> 
	 * 37: OBJECT_MAXDIST </br> 
	 * 38: OBJECT_START_INDEX </br> 
	 * 39: OBJECT_END_INDEX </br>
	 * 40: OBJECT_SCORE </br> 
	 * 41: CURR_TIMESTAMP
	 * </pre>
	 * 
	 * 
	 * @param existingMetadataFile
	 * @param sampleSentencesFile
	 * @param updatedMetadataFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static Map<String, Metadata> compileMetadata(File sampleSentencesFile)
			throws FileNotFoundException, IOException {

		Map<String, Metadata> predicationIdToMetadataMap = new HashMap<String, Metadata>();
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(sampleSentencesFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 1000 == 0) {
				System.out.println("progress: " + line.getLineNumber());
			}

			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %d\n", line.getLineNumber()));
				continue;
			}
			String predicationId = nextRecord[10];
			String zone = nextRecord[2];
			String subjectScore = nextRecord[31];
			String objectScore = nextRecord[40];

			String subjectCui = nextRecord[14];
			String objectCui = nextRecord[18];

			int sentenceStartIndex = Integer.parseInt(nextRecord[4]);
			String sentence = nextRecord[5];
			String subjectText = nextRecord[26];
			int subjectStartIndex = Integer.parseInt(nextRecord[29]);
			int subjectEndIndex = Integer.parseInt(nextRecord[30]);
			String objectText = nextRecord[35];
			int objectStartIndex = Integer.parseInt(nextRecord[38]);
			int objectEndIndex = Integer.parseInt(nextRecord[39]);

			Metadata m = new Metadata(predicationId, zone, subjectScore, objectScore, sentenceStartIndex, sentence,
					subjectText, subjectStartIndex, subjectEndIndex, objectText, objectStartIndex, objectEndIndex,
					subjectCui, objectCui);

			predicationIdToMetadataMap.put(predicationId, m);
		}

		return predicationIdToMetadataMap;

	}

	/**
	 * write the updated metadata file
	 * 
	 * @param existingMetadataFile
	 * @param updatedMetadataFile
	 * @param predicationIdToIsTreatsMap
	 * @param predicationIdToNodeNormInformationContent
	 * @param cuiToIdfMap
	 * @param predicationIdToZoneMap
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static void writeUpdatedMetadataFile(File existingMetadataFile, File updatedMetadataFile,
			Map<String, Metadata> predicationIdToMetadataMap, Map<String, Boolean> predicationIdToIsTreatsMap,
			Map<String, Float> predicationIdToNodeNormInformationContent, Map<String, Float> cuiToIdfMap)
			throws IOException, FileNotFoundException {
		System.out.println("Writing updated metadata file...");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(updatedMetadataFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(existingMetadataFile,
					CharacterEncoding.UTF_8); lineIter.hasNext();) {
				Line line = lineIter.next();

				if (line.getLineNumber() == 0) {
					// just print the header
					writer.write(String.format(
							"%s,zone,subject_score,object_score,tmkp_treats,subj_nodenorm_ic,obj_nodenorm_ic,subj_idf,obj_idf\n",
							line.getText()));
					continue;
				}

				String[] cols = line.getText().split(",");
				String predicationId = cols[0];
				Metadata m = predicationIdToMetadataMap.get(predicationId);
				boolean tmkpIsTreats = predicationIdToIsTreatsMap.get(predicationId);

				String subjectCui = m.getSubjectCui();
				String objectCui = m.getObjectCui();
				Float subj_nn_ic = null;
				Float obj_nn_ic = null;
				if (predicationIdToNodeNormInformationContent.containsKey(subjectCui)) {
					subj_nn_ic = predicationIdToNodeNormInformationContent.get(subjectCui);
				}
				if (predicationIdToNodeNormInformationContent.containsKey(objectCui)) {
					obj_nn_ic = predicationIdToNodeNormInformationContent.get(objectCui);
				}
				Float subjIdf = null;
				Float objIdf = null;
				if (cuiToIdfMap.containsKey(subjectCui)) {
					subjIdf = cuiToIdfMap.get(subjectCui);
				}
				if (cuiToIdfMap.containsKey(objectCui)) {
					objIdf = cuiToIdfMap.get(objectCui);
				}

				writer.write(String.format("%s,%s,%s,%s,%b,%f,%f,%f,%f\n", line.getText(), m.getZone(),
						m.getSubjectScore(), m.getObjectScore(), tmkpIsTreats, subj_nn_ic, obj_nn_ic, subjIdf, objIdf));

			}
		}
	}

	@Data
	private static class Metadata {
		private final String predicationId;
		private final String zone;
		private final String subjectScore;
		private final String objectScore;

		private final int sentenceStartIndex;
		private final String sentence;
		private final String subjectText;
		private final int subjectStartIndex;
		private final int subjectEndIndex;
		private final String objectText;
		private final int objectStartIndex;
		private final int objectEndIndex;

		private final String subjectCui;
		private final String objectCui;

	}

}
