package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import java.io.BufferedWriter;
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

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.treebank.SentenceTokenOnlyTreebankDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationUtil;
import edu.ucdenver.ccp.nlp.core.mention.PrimitiveSlotMention;
import edu.ucdenver.ccp.nlp.core.mention.SingleSlotFillerExpectedException;

/**
 * This code was originally designed to convert the CRAFT concept annotation
 * into a BIO NER format similar to the CONLL 2003 format. The CONLL 2003 format
 * is 4 columns. The format produced by this code is two columns, where the
 * first column is the token and the second column is the BIO tag for an entity.
 * Input to this data is the no-nested version of the CRAFT corpus that is
 * created by the ExcludeCraftNestedConcepts class (also in this package).
 *
 */
/**
 * @author bill
 *
 */
public class CraftToBioTagFormat {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	private static final String TOKEN_TYPE = "token";
	private static final String SENTENCE_TYPE = "sentence";
	private static final String BIO_LABEL_SLOT = "iob_label";

	public static void createBioFiles(File craftNoNestedBionlpBaseDir, File craftBaseDir, File outputDir)
			throws IOException {

		Map<String, File> pmidToOutputFile = getPmidToOutputFileMap(craftBaseDir, outputDir);

		Map<String, String> goToLabelMap = new HashMap<String, String>();

		Map<String, TextDocument> pmidToConceptDocMap = loadConceptAnnotations(craftNoNestedBionlpBaseDir, craftBaseDir,
				goToLabelMap);
		Map<String, TextDocument> pmidToSentenceDocMap = loadSentenceTokenAnnotations(pmidToConceptDocMap.keySet(),
				craftBaseDir);

		for (Entry<String, TextDocument> entry : pmidToConceptDocMap.entrySet()) {
			String pmid = entry.getKey();
			System.out.println("Processing " + pmid);
			TextDocument conceptDoc = entry.getValue();
			TextDocument sentenceDoc = pmidToSentenceDocMap.get(pmid);

			serialize(conceptDoc, sentenceDoc, pmidToOutputFile.get(pmid), goToLabelMap);
		}

	}

	/**
	 * Serialize the document using a BIO tag format
	 * 
	 * @param conceptDoc
	 * @param sentenceDoc
	 * @param outputFile
	 * @param goToLabelMap
	 * @throws IOException
	 */
	private static void serialize(TextDocument conceptDoc, TextDocument sentenceDoc, File outputFile,
			Map<String, String> goToLabelMap) throws IOException {

		// add a BIO label to each token annotation
		addBioLabelsToTokens(sentenceDoc, conceptDoc, goToLabelMap);
		// map tokens to the sentence in which they occur
		Map<TextAnnotation, Set<TextAnnotation>> sentenceToTokenMap = getSentenceToTokenMap(sentenceDoc);
		// sort the sentences so we output in the correct order
		List<TextAnnotation> sentenceAnnots = new ArrayList<TextAnnotation>(sentenceToTokenMap.keySet());
		Collections.sort(sentenceAnnots, TextAnnotation.BY_SPAN());

		// serialize each sentence, with a blank line in between
		outputFile.getParentFile().mkdirs();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			for (TextAnnotation sentenceAnnot : sentenceAnnots) {
				serializeTokensForSentence(writer, sentenceToTokenMap.get(sentenceAnnot));
			}
		}

	}

	/**
	 * Serialize the tokens for a single sentence, end with a blank line
	 * 
	 * @param writer
	 * @param set
	 */
	private static void serializeTokensForSentence(BufferedWriter writer, Set<TextAnnotation> tokens)
			throws IOException {
		List<TextAnnotation> orderedTokens = new ArrayList<TextAnnotation>(tokens);
		Collections.sort(orderedTokens, TextAnnotation.BY_SPAN());

		for (TextAnnotation token : orderedTokens) {
			String coveredText = token.getCoveredText();
			String bioTag = getBioTag(token);
			writer.write(String.format("%s\t%s\n", coveredText, bioTag));
		}

		writer.write("\n");

	}

	/**
	 * Map tokens to the sentences to which they belongs
	 * 
	 * @param sentenceDoc
	 * @return
	 */
	private static Map<TextAnnotation, Set<TextAnnotation>> getSentenceToTokenMap(TextDocument sentenceDoc) {
		Map<TextAnnotation, Set<TextAnnotation>> map = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		// pre-populate the map with sentences
		for (TextAnnotation annot : sentenceDoc.getAnnotations()) {
			if (annot.getClassMention().getMentionName().equals(SENTENCE_TYPE)) {
				map.put(annot, new HashSet<TextAnnotation>());
			}
		}

		// now map tokens to sentences -- this could be optimized
		for (TextAnnotation sentenceAnnot : new HashSet<TextAnnotation>(map.keySet())) {
			for (TextAnnotation tokenAnnot : sentenceDoc.getAnnotations()) {
				if (tokenAnnot.getClassMention().getMentionName().equals(TOKEN_TYPE)) {
					if (tokenAnnot.overlaps(sentenceAnnot)) {
						CollectionsUtil.addToOne2ManyUniqueMap(sentenceAnnot, tokenAnnot, map);
					}
				}
			}
		}

		return map;
	}

	public enum LabelType {
		B, I, O
	}

	/**
	 * for each token, and the appropriate B, I, or O tag
	 * 
	 * @param sentenceDoc
	 * @param conceptDoc
	 * @param goToLabelMap
	 */
	private static void addBioLabelsToTokens(TextDocument sentenceDoc, TextDocument conceptDoc,
			Map<String, String> goToLabelMap) {
		Map<TextAnnotation, Set<TextAnnotation>> conceptToTokensMap = populateConceptToTokensMap(sentenceDoc,
				conceptDoc);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : conceptToTokensMap.entrySet()) {
			TextAnnotation conceptAnnot = entry.getKey();
			List<TextAnnotation> tokenAnnots = new ArrayList<TextAnnotation>(entry.getValue());
			Collections.sort(tokenAnnots, TextAnnotation.BY_SPAN());

			for (int i = 0; i < tokenAnnots.size(); i++) {
				TextAnnotation tokenAnnot = tokenAnnots.get(i);
				String conceptId = conceptAnnot.getClassMention().getMentionName();
				// the first token gets a B tag, the others an I tag
				if (i == 0) {
					addLabel(getLabel(LabelType.B, conceptId, goToLabelMap), tokenAnnot);
				} else {
					addLabel(getLabel(LabelType.I, conceptId, goToLabelMap), tokenAnnot);
				}
			}
		}

		// assign O label to tokens missing a B or I label
		for (TextAnnotation tokenAnnot : sentenceDoc.getAnnotations()) {
			if (tokenAnnot.getClassMention().getMentionName().equals(TOKEN_TYPE)) {
				if (!hasLabel(tokenAnnot)) {
					addLabel(LabelType.O.name(), tokenAnnot);
				}
			}
		}

	}

	/**
	 * Create a mapping from concept annotation to the tokens that overlap it
	 * 
	 * @param sentenceDoc
	 * @param conceptDoc
	 * @return
	 */
	private static Map<TextAnnotation, Set<TextAnnotation>> populateConceptToTokensMap(TextDocument sentenceDoc,
			TextDocument conceptDoc) {
		Map<TextAnnotation, Set<TextAnnotation>> map = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		for (TextAnnotation conceptAnnot : conceptDoc.getAnnotations()) {
			for (TextAnnotation tokenAnnot : sentenceDoc.getAnnotations()) {
				if (tokenAnnot.getClassMention().getMentionName().equals(TOKEN_TYPE)
						&& tokenAnnot.overlaps(conceptAnnot)) {
					CollectionsUtil.addToOne2ManyUniqueMap(conceptAnnot, tokenAnnot, map);
				}
			}
		}

		return map;
	}

	/**
	 * Add the specified BIO label to the specified token annotation
	 * 
	 * @param label
	 * @param tokenAnnot
	 */
	private static void addLabel(String label, TextAnnotation tokenAnnot) {
		if (hasLabel(tokenAnnot)) {
			String existingLabel = getBioTag(tokenAnnot);
			if (existingLabel.equals(label)) {
				// there are cases in CRAFT where one token has been assigned multiple concept
				// IDs, e.g. pmid:15836427 Er81EWS has been assigned one PR ID for Er81 and one
				// for EWS. Since the concept type is the same, we won't do anything.
				return;
			} else {
				System.out.println(String.format(
						"Token already has a BIO label. Trying to add label: %s to token with existing label %s: %s",
						label, existingLabel, tokenAnnot.getSingleLineRepresentation()));

				// prefer protein labels as it looks like these overlaps are protein for the
				// most part
				if (existingLabel.contains("PR")) {
					// don't do anything
				} else if (label.contains("PR")) {
					// then replace the label for the PR label
					System.out.println("Replaced with PR");
					tokenAnnot.getClassMention().setPrimitiveSlotMentions(new ArrayList<PrimitiveSlotMention>());
					addLabel(label, tokenAnnot);
				}

//				throw new IllegalArgumentException(String.format(
//						"Token already has a BIO label. Trying to add label: %s to token with existing label %s: %s",
//						label, existingLabel, tokenAnnot.getSingleLineRepresentation()));
			}
		} else {
			TextAnnotationUtil.addSlotValue(tokenAnnot, BIO_LABEL_SLOT, label);
		}
	}

	/**
	 * @param tokenAnnot
	 * @return the BIO tag for the specified annotation
	 */
	private static String getBioTag(TextAnnotation tokenAnnot) {
		try {
			return tokenAnnot.getClassMention().getPrimitiveSlotMentionByName(BIO_LABEL_SLOT).getSingleSlotValue()
					.toString();
		} catch (SingleSlotFillerExpectedException e) {
			System.out.println(tokenAnnot.toString());
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	/**
	 * 
	 * @param tokenAnnot
	 * @return true if the specified token annotation has a BIO_LABEL slot already
	 */
	private static boolean hasLabel(TextAnnotation tokenAnnot) {
		return null != tokenAnnot.getClassMention().getPrimitiveSlotMentionByName(BIO_LABEL_SLOT);
	}

	/**
	 * Given a concept ID and label type, return an appropriate BIO label, e.g.
	 * I-CHEBI
	 * 
	 * @param labelType
	 * @param conceptId
	 * @return
	 */
	private static String getLabel(LabelType labelType, String conceptId, Map<String, String> goToLabelMap) {
		String conceptIdPrefix = conceptId.split(":")[0];
		if (goToLabelMap.containsKey(conceptId)) {
			conceptIdPrefix = goToLabelMap.get(conceptId);
		}

		// there is one HP annotation in CRAFT - we label it as MONDO for the purposes
		// of BIO labeling
		if (conceptIdPrefix.equals("HP")) {
			conceptIdPrefix = "MONDO";
		}

		return String.format("%s-%s", labelType.name(), conceptIdPrefix);
	}

	/**
	 * Loads all annotations and returns a map from PMID to {@link TextDocument}
	 * 
	 * @param craftNoNestedBionlpBaseDir
	 * @param craftBaseDir
	 * @return
	 * @throws IOException
	 */
	private static Map<String, TextDocument> loadConceptAnnotations(File craftNoNestedBionlpBaseDir, File craftBaseDir,
			Map<String, String> goToLabelMap) throws IOException {
		Map<String, TextDocument> map = new HashMap<String, TextDocument>();

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		for (Iterator<File> fileIter = FileUtil.getFileIterator(craftNoNestedBionlpBaseDir, true, "bionlp"); fileIter
				.hasNext();) {
			File bionlpFile = fileIter.next();
			String docId = bionlpFile.getName().split("\\.")[0];
			File txtFile = ExcludeCraftNestedConcepts.getTextFile(docId, craftBaseDir);

			String parentDirName = bionlpFile.getParentFile().getName();
			/* SKIP GO_MF */
			if (!parentDirName.equals("go_mf")) {
				TextDocument td = bionlpReader.readDocument(docId, "craft", bionlpFile, txtFile, ENCODING);

				if (parentDirName.equals("go_bp") || parentDirName.equals("go_cc") || parentDirName.equals("go_mf")) {
					for (TextAnnotation annot : td.getAnnotations()) {
						String id = annot.getClassMention().getMentionName();
						if (id.contains("GO:")) {
							goToLabelMap.put(id, parentDirName.toUpperCase());
						}
					}
				}

				if (!map.containsKey(docId)) {
					map.put(docId, td);
				} else {
					map.get(docId).addAnnotations(td.getAnnotations());
				}
			}
		}

		return map;
	}

	/**
	 * Map PMID to a document containing the sentence and token annotations
	 * 
	 * @param pmids
	 * @param craftBaseDir
	 * @return
	 * @throws IOException
	 */
	private static Map<String, TextDocument> loadSentenceTokenAnnotations(Set<String> pmids, File craftBaseDir)
			throws IOException {
		Map<String, TextDocument> map = new HashMap<String, TextDocument>();

		for (String pmid : pmids) {
			File txtFile = ExcludeCraftNestedConcepts.getTextFile(pmid, craftBaseDir);
			File treebankFile = getTreebankFile(pmid, craftBaseDir);

			// add sentence & token annotations here
			SentenceTokenOnlyTreebankDocumentReader dr = new SentenceTokenOnlyTreebankDocumentReader();
			TextDocument treebankDoc = dr.readDocument(pmid, "craft", treebankFile, txtFile, ENCODING);

			// re-label token annotations with "token" instead of the part-of-speech
			for (TextAnnotation annot : treebankDoc.getAnnotations()) {
				if (!annot.getClassMention().getMentionName().equals(SENTENCE_TYPE)) {
					annot.getClassMention().setMentionName(TOKEN_TYPE);
				}
			}

			map.put(pmid, treebankDoc);
		}
		return map;
	}

	/**
	 * Get the treebank file for the specified PMID
	 * 
	 * @param pmid
	 * @param craftBaseDir
	 * @return
	 */
	private static File getTreebankFile(String pmid, File craftBaseDir) {
		File treebankDirectory = new File(craftBaseDir,
				String.format("structural-annotation%streebank%spenn", File.separator, File.separator));
		File treebankFile = new File(treebankDirectory, pmid + ".tree");
		return treebankFile;
	}

	/**
	 * Create a mapping from pmid to the train/dev/test output directory
	 * 
	 * @param craftBaseDir
	 * @param outputDir
	 * @return
	 * @throws IOException
	 */
	private static Map<String, File> getPmidToOutputFileMap(File craftBaseDir, File outputDir) throws IOException {
		Map<String, File> map = new HashMap<String, File>();

		File trainDir = new File(outputDir, "train");
		File devDir = new File(outputDir, "dev");
		File testDir = new File(outputDir, "test");
		File idDir = new File(craftBaseDir, String.format("articles%sids", File.separator));

		CharacterEncoding encoding = CharacterEncoding.UTF_8;
		Set<String> trainIds = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(new File(idDir, "craft-ids-train.txt"), encoding));
		Set<String> devIds = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(new File(idDir, "craft-ids-dev.txt"), encoding));
		Set<String> testIds = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(new File(idDir, "craft-ids-test.txt"), encoding));

		for (String id : trainIds) {
			File outputFile = new File(trainDir, String.format("%s.iob", id));
			map.put(id, outputFile);
		}
		for (String id : devIds) {
			File outputFile = new File(devDir, String.format("%s.iob", id));
			map.put(id, outputFile);
		}
		for (String id : testIds) {
			File outputFile = new File(testDir, String.format("%s.iob", id));
			map.put(id, outputFile);
		}

		return map;
	}

	public static void main(String[] args) {
		File craftBaseDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git");
		// the -for-crf directory has had all overlaps removed so that we can try
		// training a single CRF (or other model) to do NER
		File craftNoNestedBionlpBaseDir = new File(
				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft-shared-tasks.git/bionlp-no-nested-for-crf");
		File outputDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/iob");
		try {
			createBioFiles(craftNoNestedBionlpBaseDir, craftBaseDir, outputDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
