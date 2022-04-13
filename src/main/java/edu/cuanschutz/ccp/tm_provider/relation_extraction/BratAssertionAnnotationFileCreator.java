package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractedSentence;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;
import lombok.Data;

/**
 * Takes as input a file containing sentences pulled using the
 * SentenceExtractionPipeline which produces a TSV file containing metadata that
 * accompanies each sentence/assertion.
 * 
 * Originally designed to read output from the SentenceExtractionPipeline and
 * create a files to be used by the BRAT annotation tool.
 *
 */
@Data
public class BratAssertionAnnotationFileCreator {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	/**
	 * To avoid the annotator having to switch pages in BRAT after annotating each
	 * sentence, we will include multiple sentences on each page
	 */
	private static final int SENTENCES_PER_PAGE = 20;

	private static Set<String> IDENTIFIERS_TO_EXCLUDE = CollectionsUtil.createSet("CHEBI:36080", "CL:0000000",
			"PR:000000001", "MONDO:0000001", "DRUGBANK:DB00118");

	private static int spanOffset = 0;

	/**
	 * directory where BRAT .ann and .txt files will be stored once they are created
	 */
	private final File outputDirectory;

	public BratAssertionAnnotationFileCreator(File outputDirectory) throws IOException {
		this.outputDirectory = outputDirectory;
	}

	/**
	 * @param credentialsFile
	 * @param biolinkAssociation
	 * @param batchId             identifier for this batch - will be appended to
	 *                            the sheet title (and therefore sheet file name) to
	 *                            make it unique
	 * @param batchSize
	 * @param inputSentenceFile
	 * @param outputSpreadsheetId
	 * @param previousSubsetFiles
	 * @param includeInverse      if true, then the subject and object entities are
	 *                            the same type, so we need to output the inverse
	 *                            (switch subject and object) for each sentence
	 * @throws IOException
	 * @throws GeneralSecurityException
	 * @throws InterruptedException
	 */
	public void createBratFiles(BiolinkAssociation biolinkAssociation, String batchId, int batchSize,
			List<File> inputSentenceFiles, File previousSentenceIdsFile, Set<String> idsToExclude) throws IOException {

		Set<String> alreadyAnnotated = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(previousSentenceIdsFile, UTF8));

		int maxSentenceCount = countSentences(inputSentenceFiles);

		System.out.println("Max sentence count: " + maxSentenceCount);

		Set<Integer> indexesForNewBatch = getRandomIndexes(maxSentenceCount, batchSize);

		Set<String> hashesOutputInThisBatch = new HashSet<String>();
		// this count is used to track when a batch has been completed
//		int extractedSentenceCount = 0;
		// this count is used to track the number of sentences that have been iterated
		// over -- this number is matched to the randomly generated indexes to select
		// sentence to include in the BRAT output
		int sentenceCount = 1;
		String subBatchId = getSubBatchId(0);
		int subBatchIndex = 0;
		BufferedWriter annFileWriter = getAnnFileWriter(biolinkAssociation, batchId, subBatchId);
		BufferedWriter txtFileWriter = getTxtFileWriter(biolinkAssociation, batchId, subBatchId);
		// the idFile is not a BRAT file, but will be used to keep track of the
		// sentences that are being annotated and for any other metadata we might
		// need/want to store
		BufferedWriter idFileWriter = null;// getIdFileWriter(biolinkAssociation, batchId, subBatchId);
		try {
			int annIndex = 1;

			for (File inputSentenceFile : inputSentenceFiles) {
				System.out.println("pulling data from: " + inputSentenceFile.getName());
				InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
						? new GZIPInputStream(new FileInputStream(inputSentenceFile))
						: new FileInputStream(inputSentenceFile);

				for (Iterator<Set<ExtractedSentence>> sentIter = new SentenceIterator(is); sentIter.hasNext();) {
					Set<ExtractedSentence> sentences = sentIter.next();
					sentences = excludeBasedOnEntityIds(sentences, idsToExclude);
					if (!sentences.isEmpty() && indexesForNewBatch.contains(sentenceCount)) {
						String hash = computeHash(sentences.iterator().next());
						if (hashesOutputInThisBatch.contains(hash)) {
							throw new IllegalStateException("duplicate hash observed!");
						}
						if (!alreadyAnnotated.contains(hash)) {
							hashesOutputInThisBatch.add(hash);
							annIndex = writeSentenceToBratFiles(sentences, annIndex, annFileWriter, txtFileWriter,
									idFileWriter, biolinkAssociation);
//								extractedSentenceCount++;

							// create a new "page" of sentences at regular intervals by creating new
							// annFile, txtFile, and idFile.
							if (hashesOutputInThisBatch.size() % SENTENCES_PER_PAGE == 0
									&& hashesOutputInThisBatch.size() < batchSize) {
								// without this check for extractedSentenceCount < batchSize an empty file gets
								// created at the end of processing
								annFileWriter.close();
								txtFileWriter.write("DONE\n");
								txtFileWriter.close();
								if (idFileWriter != null) {
									idFileWriter.close();
								}
								subBatchId = getSubBatchId(++subBatchIndex);

								annFileWriter = getAnnFileWriter(biolinkAssociation, batchId, subBatchId);
								txtFileWriter = getTxtFileWriter(biolinkAssociation, batchId, subBatchId);
//									idFileWriter = getIdFileWriter(biolinkAssociation, batchId, subBatchId);
								annIndex = 1;
								spanOffset = 0;
							}
							if (hashesOutputInThisBatch.size() >= batchSize) {
								break;
							}
						}
					}
					sentenceCount++;

				}
				if (hashesOutputInThisBatch.size() >= batchSize) {
					break;
				}

			}

		} finally {
			System.out.println("closing files (" + subBatchId + "). count = " + hashesOutputInThisBatch.size());
			annFileWriter.close();
			txtFileWriter.write("DONE\n");
			txtFileWriter.close();
			if (idFileWriter != null) {
				idFileWriter.close();
			}
		}

		System.out.println("Indexes for new batch count: " + indexesForNewBatch.size());
		System.out.println("Sentence count: " + sentenceCount);
//		System.out.println("Extracted sentence count: " + extractedSentenceCount);
		System.out.println("Hash output count: " + hashesOutputInThisBatch.size());

		/*
		 * save the hashes for sentences that were output during this batch to the file
		 * that tracks sentence hashes that have already been exported to a sheet for
		 * annotation
		 */
		try (BufferedWriter alreadyAnnotatedWriter = FileWriterUtil.initBufferedWriter(previousSentenceIdsFile, UTF8,
				WriteMode.APPEND, FileSuffixEnforcement.OFF)) {
			for (String hash : hashesOutputInThisBatch) {
				alreadyAnnotatedWriter.write(hash + "\n");
			}
		}

	}

	/**
	 * @param index
	 * @return a 3-letter code in alpha order based on the input index number, e.g.
	 *         0 = aaa, 1 = aab
	 */
	static String getSubBatchId(int index) {
		char[] c = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
				'u', 'v', 'w', 'x', 'y', 'z' };

		// 0 = aaa 0,0,0
		// 1 = aab 0,0,1
		// 2 = aac 0,0,2

		int index3 = index % 26;
		int index2 = (index / 26) % 26;
		int index1 = (index / (26 * 26)) % 26;

		return "" + c[index1] + c[index2] + c[index3];
	}

	private Set<ExtractedSentence> excludeBasedOnEntityIds(Set<ExtractedSentence> sentences, Set<String> idsToExclude) {
		Set<ExtractedSentence> sentencesToReturn = new HashSet<ExtractedSentence>();

		Set<String> allIdsToExclude = new HashSet<String>(idsToExclude);
		allIdsToExclude.addAll(IDENTIFIERS_TO_EXCLUDE);
		for (ExtractedSentence sentence : sentences) {
			if (validateSubjectObject(sentence, allIdsToExclude)) {
				sentencesToReturn.add(sentence);
			}
		}

		return sentencesToReturn;
	}

	/**
	 * The assumption that all permutations of a given sentence occur in a single
	 * group in the sentence file is wrong, so we need to look at a window of
	 * sentences to ensure that we capture them all before returning the next
	 * Set<ExtractedSentence>.
	 *
	 */
	static class SentenceIterator implements Iterator<Set<ExtractedSentence>> {

		private final StreamLineIterator lineIter;

		private Map<String, Set<ExtractedSentence>> sentenceToExtractedSentenceMap = new HashMap<String, Set<ExtractedSentence>>();
		private Map<String, Long> sentenceToIndexMap = new HashMap<String, Long>();

		private static final int windowSize = 200;
		private static final int threshold = 100;
		private long currentLine = 0;

		public SentenceIterator(File sentenceFile) throws FileNotFoundException, IOException {
			this((sentenceFile.getName().endsWith(".gz")) ? new GZIPInputStream(new FileInputStream(sentenceFile))
					: new FileInputStream(sentenceFile));
		}

		public SentenceIterator(InputStream sentenceStream) throws FileNotFoundException, IOException {
			lineIter = new StreamLineIterator(sentenceStream, UTF8, null);
			fillMap();
		}

		private void fillMap() {
			int count = 0;
			while (lineIter.hasNext()) {
				Line line = lineIter.next();
				currentLine = line.getLineNumber();

				ExtractedSentence sentence = ExtractedSentence.fromTsv(line.getText(), true);
				CollectionsUtil.addToOne2ManyUniqueMap(sentence.getSentenceText(), sentence,
						sentenceToExtractedSentenceMap);
				sentenceToIndexMap.put(sentence.getSentenceText(), currentLine);

				if (count++ >= windowSize) {
					break;
				}
			}
		}

		@Override
		public boolean hasNext() {
			return !sentenceToExtractedSentenceMap.isEmpty();
		}

		@Override
		public Set<ExtractedSentence> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			// return the sentence whose index is furthest away from the current line.
			Map<String, Long> sortedMap = CollectionsUtil.sortMapByValues(sentenceToIndexMap, SortOrder.ASCENDING);
			String firstKey = sortedMap.keySet().iterator().next();

			Set<ExtractedSentence> data = sentenceToExtractedSentenceMap.remove(firstKey);
			sentenceToIndexMap.remove(firstKey);

			String secondKey = sortedMap.keySet().iterator().next();
			if ((currentLine - sortedMap.get(secondKey)) < threshold) {
				fillMap();
			}
			return data;
		}

	}

//	private BufferedWriter getIdFileWriter(BiolinkAssociation biolinkAssociation, String batchId, String subBatchIndex)
//			throws FileNotFoundException {
//		return FileWriterUtil
//				.initBufferedWriter(getIdFile(outputDirectory, biolinkAssociation, batchId, subBatchIndex));
//	}

	private BufferedWriter getTxtFileWriter(BiolinkAssociation biolinkAssociation, String batchId, String subBatchIndex)
			throws FileNotFoundException {
		return FileWriterUtil
				.initBufferedWriter(getTxtFile(outputDirectory, biolinkAssociation, batchId, subBatchIndex));
	}

	private BufferedWriter getAnnFileWriter(BiolinkAssociation biolinkAssociation, String batchId, String subBatchIndex)
			throws FileNotFoundException {
		return FileWriterUtil
				.initBufferedWriter(getAnnFile(outputDirectory, biolinkAssociation, batchId, subBatchIndex));
	}

	static File getIdFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			String subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".id");
	}

	static File getTxtFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			String subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".txt");
	}

	static File getAnnFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			String subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".ann");
	}

	private static String getFilePrefix(BiolinkAssociation biolinkAssociation, String batchId, String subBatchIndex) {
		return String.format("%s_%s_%s", biolinkAssociation.name().toLowerCase(), batchId, subBatchIndex);
	}

	/**
	 * simple filtering of sentences based on subject/object identifiers
	 * 
	 * @param sentence
	 * @return
	 */
	private boolean validateSubjectObject(ExtractedSentence sentence, Set<String> idsToExclude) {
		return !(idsToExclude.contains(sentence.getEntityId1()) || idsToExclude.contains(sentence.getEntityId2()));
	}

	/**
	 * @param sentenceId
	 * @param sentences          this may be more than one ExtractedSentence since
	 *                           each ExtractedSentence contains only a single pair
	 *                           of entities. If the original sentences had 3
	 *                           entities, then there will be 2 corresponding
	 *                           ExtractedSentences.
	 * @param annIndex
	 * @param annFileWriter
	 * @param txtFileWriter
	 * @param idFileWriter
	 * @param biolinkAssociation
	 * @param swapSubjectObject
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private int writeSentenceToBratFiles(Set<ExtractedSentence> sentences, int annIndex, BufferedWriter annFileWriter,
			BufferedWriter txtFileWriter, BufferedWriter idFileWriter, BiolinkAssociation biolinkAssociation)
			throws IOException {

		int currentAnnIndex = annIndex;
		/* write the sentence to the txt file as a new line */
		ExtractedSentence sentence = sentences.iterator().next();
		txtFileWriter.write(sentence.getSentenceText() + "\n");

		/* write the sentenceId, documentId, sentenceWithPlaceholders to the id file */
		if (idFileWriter != null) {
			String sentenceId = computeHash(sentence);
			idFileWriter.write(sentenceId + "\t" + sentence.getDocumentId() + "\n");
		}

		List<TextAnnotation> annots = getEntityAnnotations(sentences, biolinkAssociation);
		Collections.sort(annots, TextAnnotation.BY_SPAN());

		/* write the entity annotations to the ann file */
		for (TextAnnotation annot : annots) {
			String tIndex = "T" + currentAnnIndex++;
			String label = annot.getClassMention().getMentionName().toLowerCase();
			String spanStr = getSpanStr(annot);
			String coveredText = annot.getCoveredText();

			String annLine = String.format("%s\t%s %s\t%s", tIndex, label, spanStr, coveredText);
			annFileWriter.write(annLine + "\n");
		}

		spanOffset += sentence.getSentenceText().length() + 1;
		return currentAnnIndex;
	}

	private List<TextAnnotation> getEntityAnnotations(Set<ExtractedSentence> sentences,
			BiolinkAssociation biolinkAssociation) {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		Set<TextAnnotation> annots = new HashSet<TextAnnotation>();
		for (ExtractedSentence sentence : sentences) {

			TextAnnotation subjectAnnot = getSubjectAnnot(biolinkAssociation, factory, sentence);
			annots.add(subjectAnnot);

			TextAnnotation objectAnnot = getObjectAnnot(biolinkAssociation, factory, sentence);
			annots.add(objectAnnot);

		}

		return new ArrayList<TextAnnotation>(annots);
	}

	private TextAnnotation getSubjectAnnot(BiolinkAssociation biolinkAssociation, TextAnnotationFactory factory,
			ExtractedSentence sentence) {
		String subjectType = biolinkAssociation.getSubjectPlaceholder().substring(1,
				biolinkAssociation.getSubjectPlaceholder().length() - 1);
		getSubjectText(sentence, biolinkAssociation);
		List<Span> subjectSpans = getSubjectSpan(sentence, biolinkAssociation);

		TextAnnotation annot = factory.createAnnotation(subjectSpans, sentence.getSentenceText(),
				new DefaultClassMention(subjectType));
		return annot;
	}

	private TextAnnotation getObjectAnnot(BiolinkAssociation biolinkAssociation, TextAnnotationFactory factory,
			ExtractedSentence sentence) {
		String objectType = biolinkAssociation.getObjectPlaceholder().substring(1,
				biolinkAssociation.getObjectPlaceholder().length() - 1);
		getObjectText(sentence, biolinkAssociation);
		List<Span> objectSpans = getObjectSpan(sentence, biolinkAssociation);

		TextAnnotation annot = factory.createAnnotation(objectSpans, sentence.getSentenceText(),
				new DefaultClassMention(objectType));
		return annot;
	}

	private String getSpanStr(TextAnnotation annot) {
		List<Span> spans = annot.getSpans();
		Collections.sort(spans, Span.ASCENDING());
		StringBuilder sb = new StringBuilder();
		for (Span span : spans) {
			if (sb.length() > 0) {
				sb.append(";");
			}
			sb.append((span.getSpanStart() + spanOffset) + " " + (span.getSpanEnd() + spanOffset));
		}
		return sb.toString();
	}

	private String getSubjectText(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String subjectPlaceholder = biolinkAssociation.getSubjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
			return sentence.getEntityCoveredText1();
		}
		return sentence.getEntityCoveredText2();
	}

	private String getObjectText(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String objectPlaceholder = biolinkAssociation.getObjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
			return sentence.getEntityCoveredText1();
		}
		return sentence.getEntityCoveredText2();
	}

	private static List<Span> getSubjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String subjectPlaceholder = biolinkAssociation.getSubjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
			return sentence.getEntitySpan1();
		}
		return sentence.getEntitySpan2();
	}

	private static List<Span> getObjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String objectPlaceholder = biolinkAssociation.getObjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
			return sentence.getEntitySpan1();
		}
		return sentence.getEntitySpan2();
	}

//	private static String getSubjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String subjectPlaceholder = biolinkAssociation.getSubjectPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
//			return sentence.getEntityId1();
//		}
//		return sentence.getEntityId2();
//	}
//
//	private static String getObjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String objectPlaceholder = biolinkAssociation.getObjectPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
//			return sentence.getEntityId1();
//		}
//		return sentence.getEntityId2();
//	}

	protected static String computeHash(ExtractedSentence sentence) {
		return DigestUtil.getBase64Sha1Digest(sentence.getSentenceText());
	}

	/**
	 * @param inputSentenceFile
	 * @return the number of sentences in the specified file -- this is not just the
	 *         line count, but the count of unique sentences. Many sentences appear
	 *         multiple times (on multiple lines) because the contain multiple
	 *         entities. This method does assume that the different entries for each
	 *         particular sentence appear on consecutive lines in the input file.
	 */
	protected static int countSentences(List<File> inputSentenceFiles) throws IOException {
		int sentenceCount = 0;
		String previousSentenceText = null;

		for (File inputSentenceFile : inputSentenceFiles) {
			try (InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
					? new GZIPInputStream(new FileInputStream(inputSentenceFile))
					: new FileInputStream(inputSentenceFile)) {

				for (StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null); lineIter.hasNext();) {
					Line line = lineIter.next();
					String[] cols = line.getText().split("\\t");
					String sentenceText = cols[10];
					if (previousSentenceText == null || !previousSentenceText.equals(sentenceText)) {
						sentenceCount++;
						previousSentenceText = sentenceText;
					}
				}
			}
		}
		return sentenceCount;
	}

	protected static Set<Integer> getRandomIndexes(int maxSentenceCount, int batchSize) {
		Set<Integer> randomIndexes = new HashSet<Integer>();

		// add 100 extra just in case there are collisions with previous extracted
		// sentences
		Random rand = new Random();
		while (randomIndexes.size() < maxSentenceCount && randomIndexes.size() < batchSize + 10000) {
			randomIndexes.add(rand.nextInt(maxSentenceCount) + 1);

		}

		return randomIndexes;
	}

	@Data
	private static class SentencePayload {
		private final Set<ExtractedSentence> nextSentence;
		private final ExtractedSentence waitingSentence;
	}
}
