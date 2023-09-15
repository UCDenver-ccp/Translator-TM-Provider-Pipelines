package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Produces a snippet of text that will be used to augment the original document
 * text. The snippet will eventually be added to the bottom of the original
 * document text so that it can be processed by different tools. In particular,
 * the augmented portion will contain sentences with abbreviation definitions
 * where the short form part of the abbreviation has been removed from the
 * sentence to the bottom so that they can be processed by the concept
 * recognition components.
 * 
 * We store the augmented portion separately to avoid duplicate storage of the
 * original document text (again -- it is already duplicated in the sentence
 * storage, and maybe the section annotations too).
 * 
 * This code also creates an similar augmented snippet for the sentence bionlp
 * documents since the sentence documents are also processed by downstream
 * components.
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DocumentTextAugmentationFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	public static final String AUGMENTED_SENTENCE_INDICATOR = "AUGSENT";

	public static final String AUG_SENT_TYPE = "augmented_sentence";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> AUGMENTED_TEXT_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> AUGMENTED_SENTENCE_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content,
			DocumentCriteria outputDocCriteria, Set<DocumentCriteria> requiredDocumentCriteria,
			com.google.cloud.Timestamp timestamp, PipelineKey pipelineKey) {

		return statusEntity2Content.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (PipelineMain.requiredDocumentsArePresent(docs.keySet(), requiredDocumentCriteria,
									pipelineKey, ETL_FAILURE_TAG, docId, outputDocCriteria, timestamp, out)) {
//							if (!PipelineMain.fulfillsRequiredDocumentCriteria(docs.keySet(),
//									requiredDocumentCriteria)) {
//								PipelineMain.logFailure(ETL_FAILURE_TAG,
//										"Unable to complete doc text augmentation due to missing annotation documents for: "
//												+ docId + " -- contains (" + docs.size() + ") "
//												+ docs.keySet().toString(),
//										outputDocCriteria, timestamp, out, docId, null);
//							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(), docs);

								Collection<TextAnnotation> abbrevAnnots = docTypeToContentMap
										.get(DocumentType.ABBREVIATIONS);

								Collection<TextAnnotation> sentenceAnnots = docTypeToContentMap
										.get(DocumentType.SENTENCE);

								String documentText = PipelineMain.getDocumentText(docs, docId);

								// there are some abstracts with no title and not abstract text, e.g.,
								// PMID:37000644 -- they will have a null or empty sentenceAnnots collection so
								// we account for them here. We won't output the augmented text or sentences in
								// order to prevent further downstream processing.
								if (sentenceAnnots != null && sentenceAnnots.size() > 0) {
									String[] augmentedDocSent = getAugmentedDocumentTextAndSentenceBionlp(documentText,
											abbrevAnnots, sentenceAnnots);

									List<String> chunkedAugDocText = PipelineMain.chunkContent(augmentedDocSent[0]);
									out.get(AUGMENTED_TEXT_TAG).output(KV.of(statusEntity, chunkedAugDocText));
									List<String> chunkedAugSentText = PipelineMain.chunkContent(augmentedDocSent[1]);
									out.get(AUGMENTED_SENTENCE_TAG).output(KV.of(statusEntity, chunkedAugSentText));
								}
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during document text augmentation. ",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withOutputTags(AUGMENTED_TEXT_TAG, TupleTagList.of(ETL_FAILURE_TAG).and(AUGMENTED_SENTENCE_TAG)));
	}

	/**
	 * 
	 * Initially designed to add text to the bottom of the original text document.
	 * 
	 * This method was modified so that it returns the text to be added at the
	 * bottom of the original document text as well as sentences in bionlp format
	 * for the added sentences.
	 * 
	 * The added text is sentences that contain abbreviation definitions where the
	 * short form of the definition has been removed. We do this so that these
	 * sentences can be processed by the concept recognition machinery in hopes of
	 * achieving more matches when the short form is not present. The short form
	 * part of each abbreviation definition is replaced with spaces so that the
	 * sentence spans remain consistent with the original document.
	 * 
	 * @param documentText
	 * @param abbrevAnnots
	 * @param sentenceAnnots
	 * @return
	 * @throws IOException
	 */
	protected static String[] getAugmentedDocumentTextAndSentenceBionlp(String documentText,
			Collection<TextAnnotation> abbrevAnnots, Collection<TextAnnotation> sentenceAnnots) throws IOException {

		StringBuilder augTextBuilder = new StringBuilder();
		List<TextAnnotation> augSentences = new ArrayList<TextAnnotation>();

		augTextBuilder.append(String.format("\n%s\n", UtilityOgerDictFileFactory.DOCUMENT_END_MARKER));

		List<TextAnnotation> sortedSentenceAnnots = new ArrayList<TextAnnotation>(sentenceAnnots);
		Collections.sort(sortedSentenceAnnots, TextAnnotation.BY_SPAN());
		int sentenceCount = sortedSentenceAnnots.size();

		Set<TextAnnotation> longFormAnnots = ConceptPostProcessingFn.getLongFormAnnots(abbrevAnnots);
		List<TextAnnotation> sortedLongFormAnnots = new ArrayList<TextAnnotation>(longFormAnnots);
		// we sort the abbreviation annotations by span so that the output is
		// deterministic
		Collections.sort(sortedLongFormAnnots, TextAnnotation.BY_SPAN());

		// for each abbreviation definition, retrieve the sentence in which it appears
		// and add the sentence to the augmented document text such that the short form
		// of the abbreviation is replaced by whitespace. For example, replace
		// "embryonic stem (ES) cells" with "embryonic ____ stem cells".
		for (TextAnnotation longFormAnnot : sortedLongFormAnnots) {

			TextAnnotation shortFormAnnot = ConceptPostProcessingFn.getShortAbbrevAnnot(longFormAnnot);
			TextAnnotation sentenceAnnot = getOverlappingSentenceAnnot(longFormAnnot, shortFormAnnot,
					sortedSentenceAnnots);

			if (sentenceAnnot != null) {

				// we need to replace the surrounding parentheses (and be flexible to allow for
				// different kinds of brackets even though we expect parentheses most of the
				// time) -- for now we will look at the character position immediately prior to
				// and trailing the short form and if it is punctuation, then we will include
				// it as part of the short form.

				// note that typically the short form occurs after the long form, and the short
				// form is surrounded by parentheses, however this is not always the case, e.g.
				// PMID:37021569. In this abstract the long form occurs after the short form and
				// is in parentheses.

				String sentenceText = documentText.substring(sentenceAnnot.getAnnotationSpanStart(),
						sentenceAnnot.getAnnotationSpanEnd());

				if (shortFormAnnot.getAnnotationSpanStart() > longFormAnnot.getAnnotationSpanEnd()) {
					addSfTrailingLfAugSentence(documentText, augTextBuilder, augSentences, longFormAnnot,
							shortFormAnnot, sentenceAnnot, sentenceText);
				} else {
					addSfPrecedingLfAugSentence(documentText, augTextBuilder, augSentences, longFormAnnot,
							shortFormAnnot, sentenceAnnot, sentenceText);
				}
			}
		}

//		String augDocText = documentText + sb.toString();
		String augDocText = augTextBuilder.toString();

		TextDocument td = new TextDocument("12345", "", augDocText);
		td.addAnnotations(augSentences);

		BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		bionlpWriter.serialize(td, outStream, CharacterEncoding.UTF_8);
		String augSentBionlp = outStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());

		// we need to adjust the T indexes so that they are indexed according to how
		// many sentences are also in the original document
		StringBuilder augBionlpBuilder = new StringBuilder();
		for (StreamLineIterator lineIter = new StreamLineIterator(new ByteArrayInputStream(augSentBionlp.getBytes()),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String updatedLine = String.format("T%d\t%s\t%s\n", ++sentenceCount, cols[1], cols[2]);
			augBionlpBuilder.append(updatedLine);
		}

		return new String[] { augDocText, augBionlpBuilder.toString() };

	}

	/**
	 * This is the non-canonical case, e.g. PMID:37021569, where the long form
	 * appears in parentheses after the short form of the abbreviation
	 * 
	 * @param documentText
	 * @param augTextBuilder
	 * @param augSentences
	 * @param longFormAnnot
	 * @param shortFormAnnot
	 * @param sentenceAnnot
	 * @param sentenceText
	 */
	private static void addSfPrecedingLfAugSentence(String documentText, StringBuilder augTextBuilder,
			List<TextAnnotation> augSentences, TextAnnotation longFormAnnot, TextAnnotation shortFormAnnot,
			TextAnnotation sentenceAnnot, String sentenceText) {
		int sentOffset = sentenceAnnot.getAnnotationSpanStart();
		int sfStart = shortFormAnnot.getAnnotationSpanStart();
		int sfEnd = shortFormAnnot.getAnnotationSpanEnd();
		int lfStart = longFormAnnot.getAnnotationSpanStart();
		int lfEnd = longFormAnnot.getAnnotationSpanEnd();

		String leadingChar = documentText.substring(lfStart - 1, lfStart);
		String trailingChar = documentText.substring(lfEnd, lfEnd + 1);

		if (leadingChar.matches("\\p{Punct}") && trailingChar.matches("\\p{Punct}")) {

			String augmentedTextStart = sentenceText.substring(0, sfStart - sentOffset);
			String augmentedTextMid = sentenceText.substring(sfEnd - sentOffset, lfStart - 1 - sentOffset);
			String augmentedTextEnd = sentenceText.substring(lfEnd + 1 - sentOffset);
			String spaceStr = getEmptyStrOfLength(sfEnd - sfStart);

			// single spaces added below replace the parentheses
			String augmentedSentenceText = augmentedTextStart + spaceStr + augmentedTextMid + " "
					+ longFormAnnot.getCoveredText() + " " + augmentedTextEnd;

			// write the augmented sentence to the StringBuilder. Each sentence will occupy
			// two lines. The first line will include the SENT indicator in the first
			// column, and the sentence start span from the original document in the second
			// column, and the abbreviation annotation start span in the 3rd column. The
			// augmented sentence text will appear on the second line.

			augTextBuilder.append(String.format("%s\t%d\t%d\t%d\n", AUGMENTED_SENTENCE_INDICATOR,
					sentenceAnnot.getAnnotationSpanStart(), shortFormAnnot.getAnnotationSpanStart(),
					longFormAnnot.getAnnotationSpanEnd()));
			augTextBuilder.append(String.format("%s\n", augmentedSentenceText));

			// create a sentence annotation for the augmented sentence that was added to the
			// document text above. It should have the span of the sentence relative to
			// where it is located in the augmented text, i.e., it should take into account
			// the length of the original document text which will be appended above the
			// augmented portion of the text.
			TextAnnotationFactory annotFactory = TextAnnotationFactory.createFactoryWithDefaults();
			// -1 to account for the trailing line break
			int end = documentText.length() + augTextBuilder.toString().length() - 1;
			int start = end - augmentedSentenceText.length();
			TextAnnotation augSentAnnot = annotFactory.createAnnotation(start, end, augmentedSentenceText,
					AUG_SENT_TYPE);
			augSentences.add(augSentAnnot);
		} else {
			// one or both of the characters leading and trailing the short-form text does
			// not match a punctuation character. Not sure what to do here, so for now we
			// will do nothing.
		}
	}

	/**
	 * Create an augmented sentence for the canonical case where the short form
	 * appears after the long form. In this case we expect the short form to be
	 * surrounded by parentheses.
	 * 
	 * @param documentText
	 * @param augTextBuilder
	 * @param augSentences
	 * @param longFormAnnot
	 * @param shortFormAnnot
	 * @param sentenceAnnot
	 * @param sentenceText
	 */
	private static void addSfTrailingLfAugSentence(String documentText, StringBuilder augTextBuilder,
			List<TextAnnotation> augSentences, TextAnnotation longFormAnnot, TextAnnotation shortFormAnnot,
			TextAnnotation sentenceAnnot, String sentenceText) {
		int sentOffset = sentenceAnnot.getAnnotationSpanStart();
		int sfStart = shortFormAnnot.getAnnotationSpanStart();
		int sfEnd = shortFormAnnot.getAnnotationSpanEnd();
		String leadingChar = documentText.substring(sfStart - 1, sfStart);
		String trailingChar = documentText.substring(sfEnd, sfEnd + 1);

		if (leadingChar.matches("\\p{Punct}") && trailingChar.matches("\\p{Punct}")) {

			String augmentedTextStart = sentenceText.substring(0, sfStart - 1 - sentOffset);
			String augmentedTextEnd = sentenceText.substring(sfEnd + 1 - sentOffset);
			String spaceStr = getEmptyStrOfLength(sfEnd + 1 - (sfStart - 1));

			String augmentedSentenceText = augmentedTextStart + spaceStr + augmentedTextEnd;

			// write the augmented sentence to the StringBuilder. Each sentence will occupy
			// two lines. The first line will include the SENT indicator in the first
			// column, and the sentence start span from the original document in the second
			// column, and the abbreviation annotation start span in the 3rd column. The
			// augmented sentence text will appear on the second line.

			augTextBuilder.append(String.format("%s\t%d\t%d\t%d\n", AUGMENTED_SENTENCE_INDICATOR,
					sentenceAnnot.getAnnotationSpanStart(), longFormAnnot.getAnnotationSpanStart(),
					shortFormAnnot.getAnnotationSpanEnd()));
			augTextBuilder.append(String.format("%s\n", augmentedSentenceText));

			// create a sentence annotation for the augmented sentence that was added to the
			// document text above. It should have the span of the sentence relative to
			// where it is located in the augmented text, i.e., it should take into account
			// the length of the original document text which will be appended above the
			// augmented portion of the text.
			TextAnnotationFactory annotFactory = TextAnnotationFactory.createFactoryWithDefaults();
			// -1 to account for the trailing line break
			int end = documentText.length() + augTextBuilder.toString().length() - 1;
			int start = end - augmentedSentenceText.length();
			TextAnnotation augSentAnnot = annotFactory.createAnnotation(start, end, augmentedSentenceText,
					AUG_SENT_TYPE);
			augSentences.add(augSentAnnot);
		} else {
			// one or both of the characters leading and trailing the short-form text does
			// not match a punctuation character. Not sure what to do here, so for now we
			// will do nothing.
		}
	}

	/**
	 * @param len
	 * @return a string of spaces of length len
	 */
	private static String getEmptyStrOfLength(int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			sb.append(" ");
		}

		return sb.toString();
	}

	/**
	 * return the sentence annotation that contains the abbreviation annotation. If
	 * the abbreviation spans multiple sentences then either the sentence
	 * segmentation is faulty or the abbreviation is incorrect, so we will return
	 * null.
	 * 
	 * If the abbreviation extends past the end of the sentence (likely sentence
	 * parse error), then don't return a mapping from that abbreviation to the
	 * sentence.
	 * 
	 * @param longFormAnnot
	 * @param sentenceAnnots
	 * @return
	 */
	private static TextAnnotation getOverlappingSentenceAnnot(TextAnnotation longFormAnnot,
			TextAnnotation shortFormAnnot, List<TextAnnotation> sentenceAnnots) {

		for (int i = 0; i < sentenceAnnots.size(); i++) {
			TextAnnotation sentenceAnnot = sentenceAnnots.get(i);
			if (sentenceAnnot.overlaps(longFormAnnot)
					&& longFormAnnot.getAnnotationSpanEnd() <= sentenceAnnot.getAnnotationSpanEnd()
					&& shortFormAnnot.getAnnotationSpanEnd() <= sentenceAnnot.getAnnotationSpanEnd()) {
				// check to see if the next sentence also overlaps -- if it does return null,
				// otherwise return the sentence
				if (i < sentenceAnnots.size() - 1) {
					TextAnnotation nextSentAnnot = sentenceAnnots.get(i + 1);
					if (nextSentAnnot.overlaps(longFormAnnot)) {
						return null;
					}
				}
				return sentenceAnnot;
			}
		}
		return null;
	}

}
