package edu.cuanschutz.ccp.tm_provider.etl.fn;

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
import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Takes the original document text and augments it by adding text to the
 * bottom. In particular, it adds sentences with abbreviation definitions where
 * the short form part of the abbreviation has been removed from the sentence to
 * the bottom so that they can be processed by the concept recognition
 * components.
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DocumentTextAugmentationFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	public static final String AUGMENTED_SENTENCE_INDICATOR = "AUGSENT";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> AUGMENTED_TEXT_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content,
			DocumentCriteria outputDocCriteria, Set<DocumentCriteria> requiredDocumentCriteria,
			com.google.cloud.Timestamp timestamp) {

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
							if (!docs.keySet().equals(requiredDocumentCriteria)) {
								PipelineMain.logFailure(ETL_FAILURE_TAG,
										"Unable to complete post-processing due to missing annotation documents for: "
												+ docId + " -- contains (" + docs.size() + ") "
												+ docs.keySet().toString(),
										outputDocCriteria, timestamp, out, docId, null);
							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(), docs);

								Collection<TextAnnotation> abbrevAnnots = docTypeToContentMap
										.get(DocumentType.ABBREVIATIONS);

								Collection<TextAnnotation> sentenceAnnots = docTypeToContentMap
										.get(DocumentType.SENTENCE);

								String documentText = PipelineMain.getDocumentText(docs);

								String augmentedDocText = augmentDocumentText(documentText, abbrevAnnots,
										sentenceAnnots);

								List<String> chunkedAugDocText = PipelineMain.chunkContent(augmentedDocText);
								out.get(AUGMENTED_TEXT_TAG).output(KV.of(statusEntity, chunkedAugDocText));
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during document text augmentation. ",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withOutputTags(AUGMENTED_TEXT_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * 
	 * Initially designed to add text to the bottom of the original text document.
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
	 */
	protected static String augmentDocumentText(String documentText, Collection<TextAnnotation> abbrevAnnots,
			Collection<TextAnnotation> sentenceAnnots) {

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("\n%s\n", UtilityOgerDictFileFactory.DOCUMENT_END_MARKER));

		List<TextAnnotation> sortedSentenceAnnots = new ArrayList<TextAnnotation>(sentenceAnnots);
		Collections.sort(sortedSentenceAnnots, TextAnnotation.BY_SPAN());

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
			TextAnnotation sentenceAnnot = getOverlappingSentenceAnnot(longFormAnnot, sortedSentenceAnnots);

			if (sentenceAnnot != null) {

				// we need to replace the surrounding parentheses (and be flexible to allow for
				// different kinds of brackets even though we expect parentheses most of the
				// time) -- for now we will look at the character position immediately prior to
				// and trailing the short form and if it is punctuation, then we will include
				// it as part of the short form.

				String sentenceText = documentText.substring(sentenceAnnot.getAnnotationSpanStart(),
						sentenceAnnot.getAnnotationSpanEnd());
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

					sb.append(String.format("%s\t%d\t%d\t%d\n", AUGMENTED_SENTENCE_INDICATOR,
							sentenceAnnot.getAnnotationSpanStart(), longFormAnnot.getAnnotationSpanStart(),
							shortFormAnnot.getAnnotationSpanEnd()));
					sb.append(String.format("%s\n", augmentedSentenceText));

				} else {
					// one or both of the characters leading and trailing the short-form text does
					// not match a punctuation character. Not sure what to do here, so for now we
					// will do nothing.
					continue;
				}
			}
		}

		String augDocText = documentText + sb.toString();
		return augDocText;

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
	 * null
	 * 
	 * @param longFormAnnot
	 * @param sentenceAnnots
	 * @return
	 */
	private static TextAnnotation getOverlappingSentenceAnnot(TextAnnotation longFormAnnot,
			List<TextAnnotation> sentenceAnnots) {

		for (int i = 0; i < sentenceAnnots.size(); i++) {
			TextAnnotation sentenceAnnot = sentenceAnnots.get(i);
			if (sentenceAnnot.overlaps(longFormAnnot)) {
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
