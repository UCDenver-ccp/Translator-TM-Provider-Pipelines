package edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli;

import static edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.computeSentenceIdentifier;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.ElasticsearchToBratExporter;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "batch")
public class BatchCreateCommand implements Runnable {

	private static final String BATCH_DIR_PREFIX = "batch_";

	@Option(names = { "-d",
			"--dir" }, required = true, description = "The base directory of the relation extraction git repository. "
					+ "The code will look for the following subdirectory: annotation-data/brat, which must be present.")
	private File repoBaseDir;

	@Option(names = { "-b",
			"--biolink" }, required = true, description = "Each annotation project focuses on a specific Biolink association. "
					+ "Allowable values are: bl_chemical_to_disease_or_phenotypic_feature, bl_chemical_to_gene, "
					+ "bl_disease_to_phenotypic_feature, bl_gene_regulatory_relationship, bl_gene_to_disease, "
					+ "bl_gene_loss_gain_of_function_to_disease, bl_gene_to_cellular_component, bl_gene_to_cell, "
					+ "bl_gene_to_anatomical_entity, bl_disease_or_phenotypic_feature_to_location, "
					+ "bl_biological_process_to_disease, bl_gene_to_biological_process.")
	private String biolinkAssociation;

	@Option(names = { "-a",
			"--annotator" }, required = true, description = "Name/identifier for the annotator assigned to this batch. "
					+ "This name will become a directory name so it should be a single token (no spaces).")
	private String annotatorKey;

	@Option(names = { "-n",
			"--sentence-count" }, required = false, defaultValue = "500", description = "The number of sentences to include in this batch.")
	private int sentenceCount;

	@Option(names = { "-v",
			"--overlap-sentence-percentage" }, required = false, defaultValue = "0.1", description = "The percentage of the sentences that "
					+ "should be shared across batches to facilitate measurements of inter-annotator agreement.")
	private float overlapSentencePercentage;

	@Option(names = { "-u",
			"--elastic-url" }, required = true, description = "URL to the Elasticsearch instance. Do not include 'http://'.")
	private String elasticUrl;

	@Option(names = { "-p", "--elastic-port" }, required = true, description = "Elasticsearch port.")
	private int elasticPort;

	@Option(names = { "-k", "--elastic-api-key" }, required = true, description = "Elasticsearch API key.")
	private String elasticApiKey;

	@Option(names = { "-i",
			"--elastic-index-name" }, required = false, defaultValue = "sentences", description = "Elasticsearch index name to be queried.")
	private String elasticIndexName;

	@Option(names = { "-t",
			"--batch-id" }, required = false, description = "[OPTIONAL] The name of the batch to be created. If not specified, "
					+ "then a batch name of 'batch_n', where n is an integer, will be used.")
	private String batchId;

	@Option(names = { "-s",
			"--subject-idf-threshold" }, required = false, defaultValue = "-1.0", description = "Inverse document frequency threshold for including a subject concept in a sentence to be annotated.")
	private float subjectIdfThreshold;

	@Option(names = { "-o",
			"--object-idf-threshold" }, required = false, defaultValue = "-1.0", description = "Inverse document frequency threshold for including an object concept in a sentence to be annotated.")
	private float objectIdfThreshold;

	@Option(names = { "-z",
			"--go-bp-ids-file" }, required = true, description = "Precomputed file containing GO identifiers for biological process concepts.")
	private File goBpIdsFile;

	@Option(names = { "-y",
			"--go-cc-ids-file" }, required = true, description = "Precomputed file containing GO identifiers for cellular component concepts.")
	private File goCcIdsFile;

	@Option(names = { "-f",
			"--concept-idf-file" }, required = true, description = "File with IDF values for all concepts.")
	private File conceptIdfFile;

	@Option(names = { "-g",
			"--sentences-per-page" }, required = false, defaultValue = "20", description = "The number of sentences in each BRAT file, i.e., how many will show up on a single page in the BRAT UI")
	private int sentencesPerPage;

	@Override
	public void run() {
		createBatch();
	}

	private void createBatch() {
		BiolinkAssociation association = BiolinkAssociation.valueOf(biolinkAssociation.toUpperCase());
		File annotationDir = new File(repoBaseDir, "annotation-data");
		File bratDir = new File(annotationDir, "brat");
		checkBratDir(bratDir);
		File associationDir = new File(bratDir, association.name().toLowerCase());
		File annotatorDir = new File(associationDir, annotatorKey);
		String batchIdentifier = getBatchId(batchId, annotatorDir);

		File batchDir = getBatchDirectory(annotatorDir, batchIdentifier);

		System.out.println("Association: " + association.name());
		System.out.println("Subject class: " + association.getSubjectClass());
		System.out.println("Object class: " + association.getObjectClass());

		Set<Set<String>> ontologyPrefixes = new HashSet<Set<String>>();
		ontologyPrefixes.add(new HashSet<String>(association.getSubjectClass().getOntologyPrefixes()));
		ontologyPrefixes.add(new HashSet<String>(association.getObjectClass().getOntologyPrefixes()));

		try {
			/*
			 * If we are creating the very first annotator1/batch1 for an association, then
			 * there will be no overlapping sentences -- If we are creating the second batch
			 * with a different annotator (annotator2/batch1) of an association, there won't
			 * be any existing overlapping sentences, so we will use the percentage overlap
			 * number to choose sentences to use for the overlap. -- If there are already
			 * two annotator/batch1 instances, then there will be overlapping sentences, so
			 * we need to extract those sentences.
			 */
			List<TextDocument> redundantSentencesForAnnotation = getOverlappingSentences(associationDir,
					batchIdentifier, batchDir, overlapSentencePercentage);

			/*
			 * catalog sentence IDs that have already been extracted for annotation for this
			 * association
			 */
			Set<String> alreadyInUseSentenceIds = getAlreadyInUseSentenceIds(associationDir);

			System.out.println("REDUNDANT SENTENCE LIST SIZE: " + redundantSentencesForAnnotation.size());
			System.out.println("ALREADY IN USE SENTENCE IDS SIZE: " + alreadyInUseSentenceIds.size());
			System.out.println("OVERLAP PERCENTAGE: " + overlapSentencePercentage);
			/*
			 * aim to return 49,999 sentences from which we will randomly select sentences
			 * to annotate
			 */
			int maxReturned = 49999;

			System.out.println("Loading filter map...");
			Map<String, Set<String>> ontologyPrefixToAllowableConceptIds = loadOntologyPrefixToAllowableConceptIdsMap(
					association);
			System.out.println("Searching Elastic...");
			Set<TextDocument> searchResults = ElasticsearchToBratExporter.search(elasticUrl, elasticPort, elasticApiKey,
					elasticIndexName, maxReturned, ontologyPrefixes, ontologyPrefixToAllowableConceptIds, association,
					ElasticsearchToBratExporter.IDENTIFIERS_TO_EXCLUDE, alreadyInUseSentenceIds);

			System.out.println("Search hits returned from Elastic: " + searchResults.size());

			ElasticsearchToBratExporter.createBratFiles(batchDir, association, batchIdentifier, sentenceCount,
					sentencesPerPage, searchResults, alreadyInUseSentenceIds, redundantSentencesForAnnotation);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

	/**
	 * Creates a mapping from ontology prefix to ontology ids. If there is an entry
	 * for an ontology prefix in this map when it is used downstream, then it is
	 * used to filter the concept ids that are used. Only concept Ids present in
	 * this map will be used for a given ontology prefix. The map is loaded based on
	 * the subject and object classes, as well as the IDF threshold that has been
	 * specified. If an IDF threshold is < 0 then no filtering based on IDF is done.
	 * 
	 * @param association
	 * @return
	 * @throws IOException
	 */
	private Map<String, Set<String>> loadOntologyPrefixToAllowableConceptIdsMap(BiolinkAssociation association)
			throws IOException {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		// if the subject/object is a pairing of GO_BP and GO_CC, we cannot distinguish
		// between the two in the search, so more development would be needed.
		BiolinkClass subjectClass = association.getSubjectClass();
		BiolinkClass objectClass = association.getObjectClass();
		if (subjectClass == BiolinkClass.CELLULAR_COMPONENT && objectClass == BiolinkClass.BIOLOGICAL_PROCESS
				|| subjectClass == BiolinkClass.BIOLOGICAL_PROCESS && objectClass == BiolinkClass.CELLULAR_COMPONENT) {
			throw new IllegalArgumentException("Cannot handle GO_BP + GO_CC pairing. Further development needed.");
		}
		System.out.println("Preparing to filter by concept identifiers. Subject class: " + subjectClass.name()
				+ " Object class: " + objectClass.name());

		// if the subject IDF threshold is > 0 then find the ontology prefixes for the
		// subject class and grab classes from the concept IDF file. If the subject IDF
		// threshold is -1, then we don't need to do anything for the subject class.
		// if the subject class is GO_CC or GO_BP, then we need to first load the
		// namespace-specific class lists
		Set<String> subjInputClassIds = getInputClassIds(subjectClass);
		addConceptIdsAboveIdfThreshold(map, subjectIdfThreshold, subjectClass.getOntologyPrefixes(), subjInputClassIds,
				conceptIdfFile);

		// if the object class is GO_CC or GO_BP, then we need to first load the
		// namespace-specific class lists
		Set<String> objInputClassIds = getInputClassIds(objectClass);
		addConceptIdsAboveIdfThreshold(map, objectIdfThreshold, objectClass.getOntologyPrefixes(), objInputClassIds,
				conceptIdfFile);

		System.out.println("******************** Filter map keys: " + map.keySet().toString());

		return map;
	}

	/**
	 * Returns a map from concept prefix to ids. If the inputClassIds set is
	 * populated, then we only consider concepts from that id set. If an IDF
	 * threshold is also set, then we will threshold that input set of classes and
	 * add them to the map if they pass the idf threshold. Otherwise, the ontology
	 * prefixes set needs to be populated, and we grab all classes from the idf file
	 * that pass the idf threshold and have one of the requested ontology prefixes.
	 * If the IDF threshold is < 0 and the ontologyprefixes are set, we won't be
	 * filtering based on IDF so we don't need to do anything.
	 * 
	 * @param map
	 * @param idfThreshold
	 * @param ontologyPrefixes
	 * @param inputClassIds
	 * @param conceptIdfFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static void addConceptIdsAboveIdfThreshold(Map<String, Set<String>> map, float idfThreshold,
			Set<String> ontologyPrefixes, Set<String> inputClassIds, File conceptIdfFile)
			throws FileNotFoundException, IOException {

		// if the IDF threshold is < 0 then that is an indication that there should be
		// no filtering based on IDF
		if (inputClassIds != null && !inputClassIds.isEmpty()) {
			populatePrefixToIdMapBasedOnClassListAndIdf(map, idfThreshold, inputClassIds, conceptIdfFile);
		} else {
			// there are no inputClassIds, so return all classes with the specified ontology
			// prefixes that meet the IDF threshold
			if (idfThreshold > 0) {
				populatePrefixToIdMapBasedOnOntPrefixAndIdf(map, idfThreshold, ontologyPrefixes, conceptIdfFile);
			}
		}
	}

	/**
	 * populates the input map with prefix -to- concept ID mappings by returning all
	 * concepts in the conceptIdfFile that have one of the specified prefixes and
	 * also pass the IDF threshold
	 * 
	 * @param map
	 * @param idfThreshold
	 * @param ontologyPrefixes
	 * @param conceptIdfFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static void populatePrefixToIdMapBasedOnOntPrefixAndIdf(Map<String, Set<String>> map, float idfThreshold,
			Set<String> ontologyPrefixes, File conceptIdfFile) throws IOException, FileNotFoundException {
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(conceptIdfFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			String line = lineIter.next().getText().replaceAll("\"", "");
			String[] cols = line.split(",");
			String id = cols[0];
			String prefix = getPrefix(id);
			if (ontologyPrefixes.contains(prefix)) {
				String level = cols[1];
				float idf = Float.parseFloat(cols[2]);

				if (idf > idfThreshold && level.equals("document")) {
					CollectionsUtil.addToOne2ManyUniqueMap(prefix, id, map);
				}
			}
		}
	}

	/**
	 * populates the input map with prefix -to- concept ID mappings by returning all
	 * concepts in input class ids set that also pass the IDF threshold
	 * 
	 * @param map
	 * @param idfThreshold
	 * @param inputClassIds
	 * @param conceptIdfFile
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static void populatePrefixToIdMapBasedOnClassListAndIdf(Map<String, Set<String>> map, float idfThreshold,
			Set<String> inputClassIds, File conceptIdfFile) throws IOException, FileNotFoundException {
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(conceptIdfFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			String line = lineIter.next().getText().replaceAll("\"", "");
			String[] cols = line.split(",");
			String id = cols[0];
			if (inputClassIds.contains(id)) {
				String level = cols[1];
				float idf = Float.parseFloat(cols[2]);
				// if IDF threshold < 0 then we add to the map regardless
				if (idfThreshold < 0 || (idf > idfThreshold && level.equals("document"))) {
					String prefix = getPrefix(id);
					CollectionsUtil.addToOne2ManyUniqueMap(prefix, id, map);
				}
			}
		}
	}

	protected static String getPrefix(String id) {
		return id.split(":")[0];
	}

	private Set<String> getInputClassIds(BiolinkClass cls) throws IOException {
		System.out.println("=============== Retrieving specified class IDs for " + cls.name());
		switch (cls) {
		case BIOLOGICAL_PROCESS:
			System.out.println("Loading GO BP IDs file...");
			return new HashSet<String>(FileReaderUtil.loadLinesFromFile(goBpIdsFile, CharacterEncoding.UTF_8));
		case CELLULAR_COMPONENT:
			System.out.println("Loading GO CC IDs file...");
			return new HashSet<String>(FileReaderUtil.loadLinesFromFile(goCcIdsFile, CharacterEncoding.UTF_8));
		default:
			return null;
		}
	}

	/**
	 * For a given batch, the overlapping sentences will be the same for any pair of
	 * batches, so we only need to look at two batches to extract the overlapping
	 * sentences for that batch.
	 * 
	 * @param associationDir
	 * @param batchIdentifier
	 * @return
	 * @throws IOException
	 */
	private List<TextDocument> getOverlappingSentences(File associationDir, String batchIdentifier,
			File currentBatchDir, float percentOverlap) throws IOException {
		List<File> batchDirectories = getBatchDirectories(associationDir, batchIdentifier, currentBatchDir);

		System.out.println("BATCH DIRECTORIES: " + batchDirectories.toString());

		if (batchDirectories.size() == 0) {
			return Collections.emptyList();
		}

		if (batchDirectories.size() == 1) {
			// we are adding a second batch so we need to select some sentences for
			// redundant annotation based on the percent overlap that has been specified.
			File batchDir1 = batchDirectories.get(0);
			Map<String, TextDocument> sentenceIdToSentenceDocMap_batchDir1 = getSentenceIdToSentenceDocMap(batchDir1);
			List<TextDocument> sentences = new ArrayList<TextDocument>(sentenceIdToSentenceDocMap_batchDir1.values());
			List<Integer> randomIndexes = getRandomIndexes(sentences.size(), percentOverlap);
			System.out.println("RANDOM INDEXES: " + randomIndexes.size() + " --- " + randomIndexes.toString());
			List<TextDocument> redundantSentences = new ArrayList<TextDocument>();
			for (Integer index : randomIndexes) {
				redundantSentences.add(sentences.get(index));
			}
			return redundantSentences;
		}

		// otherwise there are at least 2 batches already so we will infer the redundant
		// sentences by looking for the sentences that match between two of the batches
		File batchDir1 = batchDirectories.get(0);
		File batchDir2 = batchDirectories.get(1);

		Map<String, TextDocument> sentenceIdToSentenceDocMap_batchDir1 = getSentenceIdToSentenceDocMap(batchDir1);
		Map<String, TextDocument> sentenceIdToSentenceDocMap_batchDir2 = getSentenceIdToSentenceDocMap(batchDir2);

		Set<String> batchDir1SentenceIds = new HashSet<String>(sentenceIdToSentenceDocMap_batchDir1.keySet());
		Set<String> batchDir2SentenceIds = new HashSet<String>(sentenceIdToSentenceDocMap_batchDir2.keySet());

		batchDir1SentenceIds.retainAll(batchDir2SentenceIds);

		List<TextDocument> overlappingSentences = new ArrayList<TextDocument>();
		for (String sentenceId : batchDir1SentenceIds) {
			overlappingSentences.add(sentenceIdToSentenceDocMap_batchDir1.get(sentenceId));
		}

		return overlappingSentences;
	}

	/**
	 * Return a set of random indexes from 0:size-1. The size of the set should be
	 * percentOverlap * size.
	 * 
	 * @param size
	 * @param percentOverlap
	 * @return
	 */
	private List<Integer> getRandomIndexes(int size, float percentOverlap) {
		Set<Integer> randomIndexes = new HashSet<Integer>();
		Random rand = new Random();
		int overlapCount = Math.round(size * percentOverlap);
		while (randomIndexes.size() < overlapCount) {
			randomIndexes.add(rand.nextInt(size));
		}
		return new ArrayList<Integer>(randomIndexes);
	}

	/**
	 * Find all directories with the batchIdentifier name under the different
	 * annotator directories in the specified association directory.
	 * 
	 * Exclude the current batch directory in the returned list since it is empty at
	 * this point.
	 * 
	 * @param associationDir
	 * @param batchIdentifier
	 * @return
	 * @throws FileNotFoundException
	 */
	private List<File> getBatchDirectories(File associationDir, String batchIdentifier, File currentBatchDir)
			throws FileNotFoundException {
		List<File> batchDirectories = new ArrayList<File>();

		Set<File> annotatorDirectories = FileUtil.getDirectories(associationDir);
		for (File annotatorDir : annotatorDirectories) {
			Set<File> batchDirs = FileUtil.getDirectories(annotatorDir);
			for (File batchDir : batchDirs) {
				if (batchDir.getName().equals(batchIdentifier)) {
					batchDirectories.add(batchDir);
				}
			}
		}

		batchDirectories.remove(currentBatchDir);

		return batchDirectories;
	}

	/**
	 * Cycle through the batch directory and output a map from sentence ID to
	 * sentence document (in the form of a @link{TextDocument})
	 * 
	 * @param batchDir
	 * @return
	 * @throws IOException
	 */
	private Map<String, TextDocument> getSentenceIdToSentenceDocMap(File batchDir) throws IOException {
		Map<String, TextDocument> sentenceIdToSentenceDocMap_batchDir1 = new HashMap<String, TextDocument>();
		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		for (Iterator<File> fileIterator = FileUtil.getFileIterator(batchDir, true, ".txt"); fileIterator.hasNext();) {
			File txtFile = fileIterator.next();
			File annFile = new File(txtFile.getParentFile(),
					StringUtil.removeSuffix(txtFile.getName(), ".txt") + ".ann");
			TextDocument td = reader.readDocument("doc-id", "doc-source", annFile, txtFile, CharacterEncoding.UTF_8);
			List<TextDocument> sentenceDocs = ElasticsearchToBratExporter.splitIntoSentences(td);

			for (TextDocument sentenceDoc : sentenceDocs) {
				String hash = computeSentenceIdentifier(sentenceDoc.getText());
				// ensure the document has annotations
				if (sentenceDoc.getAnnotations() != null) {
					sentenceIdToSentenceDocMap_batchDir1.put(hash, sentenceDoc);
				}
			}
		}
		return sentenceIdToSentenceDocMap_batchDir1;
	}

	/**
	 * Gather the sentence IDs from all sentences in all batches in the association
	 * directory
	 * 
	 * @param associationDir
	 * @return
	 * @throws IOException
	 */
	private Set<String> getAlreadyInUseSentenceIds(File associationDir) throws IOException {
		Set<String> sentenceIds = new HashSet<String>();
		for (Iterator<File> fileIterator = FileUtil.getFileIterator(associationDir, true, ".txt"); fileIterator
				.hasNext();) {
			File file = fileIterator.next();
			sentenceIds.addAll(RepoStatsCommand.getSentenceIds(file));
		}
		return sentenceIds;
	}

	/**
	 * Creates the batch directory, or errors if the directory already exists since
	 * we want to prevent overwriting of annotation files
	 * 
	 * @param annotatorDir
	 * @param batchIdentifier
	 * @return
	 */
	private File getBatchDirectory(File annotatorDir, String batchIdentifier) {
		File batchDir = new File(annotatorDir, batchIdentifier);

		if (batchDir.exists()) {
			System.err.println(
					"\n\nERROR: This batch directory already exists. Please delete manually if you want to overwrite.\n"
							+ batchDir.getAbsolutePath());
			System.exit(-1);

		} else {
			batchDir.mkdirs();
			System.out.println("BRAT files will be placed in: " + batchDir.getAbsolutePath());
		}
		return batchDir;
	}

	/**
	 * If the user-specified batch ID is set, then return it, otherwise interrogate
	 * the annotation directory and return the next logical batch ID, e.g. if the
	 * annotation directory contains batch_1/ & batch_2/, then return batch_3/
	 * 
	 * @param userSpecifiedBatchId
	 * @param annotatorDir
	 * @return
	 */
	private String getBatchId(String userSpecifiedBatchId, File annotatorDir) {
		if (userSpecifiedBatchId != null) {
			return userSpecifiedBatchId;
		}
		int i = 1;
		while (true) {
			String batchId = BATCH_DIR_PREFIX + i++;
			File batchDir = new File(annotatorDir, batchId);
			if (!batchDir.exists()) {
				return batchId;
			}
		}
	}

	private void checkBratDir(File bratDir) {
		// ensure the brat directory already exists
		if (!bratDir.exists()) {
			System.err.println(String.format(
					"The annotation-data/brat directory does not seem to exists. Expected: %s \n"
							+ "Please check the repository base directory input parameter.",
					repoBaseDir.getAbsolutePath()));
			System.exit(-1);
		}
	}

}
