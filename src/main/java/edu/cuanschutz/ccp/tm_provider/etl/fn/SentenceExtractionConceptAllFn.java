package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.string.RegExPatterns;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class SentenceExtractionConceptAllFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	@VisibleForTesting
	protected static final String X_CONCEPTS = "X";
	@VisibleForTesting
	protected static final String Y_CONCEPTS = "Y";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, ExtractedSentence>> EXTRACTED_SENTENCES_TAG = new TupleTag<KV<ProcessingStatus, ExtractedSentence>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText, Set<String> keywords,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Map<String, String> prefixToPlaceholderMap) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, ExtractedSentence>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						try {
							String documentText = PipelineMain.getDocumentText(statusEntityToText.getValue());

							Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
									.getDocTypeToContentMap(docId, statusEntityToText.getValue());

							Set<ExtractedSentence> extractedSentences = extractSentences(docId, documentText,
									docTypeToContentMap, keywords, prefixToPlaceholderMap);
							if (extractedSentences == null) {
								PipelineMain.logFailure(ETL_FAILURE_TAG,
										"Unable to extract sentences due to missing documents for: " + docId
												+ " -- contains (" + statusEntityToText.getValue().size() + ") "
												+ statusEntityToText.getValue().keySet().toString(),
										outputDocCriteria, timestamp, out, docId, null);
							} else {
								for (ExtractedSentence es : extractedSentences) {
									out.get(EXTRACTED_SENTENCES_TAG).output(KV.of(statusEntity, es));
								}
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during sentence extraction",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withOutputTags(EXTRACTED_SENTENCES_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	@VisibleForTesting
	protected static Set<ExtractedSentence> extractSentences(String documentId, String documentText,
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap, Set<String> keywords,
			Map<String, String> prefixToPlaceholderMap) throws IOException {

		Collection<TextAnnotation> sentenceAnnots = docTypeToContentMap.get(DocumentType.SENTENCE);
		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(DocumentType.CONCEPT_ALL);

		String xPrefix = null;
		String yPrefix = null;
		// there are only two prefixes in the map, e.g. CHEBI, CL, MONDO.
		for (String prefix : prefixToPlaceholderMap.keySet()) {
			if (xPrefix == null) {
				xPrefix = prefix;
			} else {
				yPrefix = prefix;
			}
		}

		// if there is only one prefix - then we are looking for sentences that have two
		// of the same kind of anntotation, e.g. two protein annotations
		if (yPrefix == null) {
			yPrefix = xPrefix;
		}

		List<TextAnnotation> conceptXAnnots = getAnnotsByPrefix(conceptAnnots, xPrefix);
		List<TextAnnotation> conceptYAnnots = getAnnotsByPrefix(conceptAnnots, yPrefix);

		Set<ExtractedSentence> extractedSentences = new HashSet<ExtractedSentence>();
		if (!conceptXAnnots.isEmpty() && !conceptYAnnots.isEmpty()) {

			Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = buildSentenceToConceptMap(
					sentenceAnnots, conceptXAnnots, conceptYAnnots);

			String xPlaceholder = prefixToPlaceholderMap.get(xPrefix);
			String yPlaceholder = prefixToPlaceholderMap.get(yPrefix);

			extractedSentences.addAll(catalogExtractedSentences(keywords, documentText, documentId,
					sentenceToConceptMap, xPlaceholder, yPlaceholder));

		}
		return extractedSentences;
	}

	/**
	 * filters the input collection of annotations by keeping only those that share
	 * the specified prefix, e.g. CHEBI
	 * 
	 * @param conceptAnnots
	 * @param xPrefix
	 * @return
	 */
	public static List<TextAnnotation> getAnnotsByPrefix(Collection<TextAnnotation> conceptAnnots, String prefix) {
		List<TextAnnotation> annots = new ArrayList<TextAnnotation>();

		for (TextAnnotation annot : conceptAnnots) {
			if (annot.getClassMention().getMentionName().startsWith(prefix)) {
				annots.add(annot);
			}
		}
		return annots;
	}

	@VisibleForTesting
	protected static Set<ExtractedSentence> catalogExtractedSentences(Set<String> keywords, String documentText,
			String documentId, Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap,
			String xPlaceholder, String yPlaceholder) {

		Set<ExtractedSentence> extractedSentences = new HashSet<ExtractedSentence>();
		for (Entry<TextAnnotation, Map<String, Set<TextAnnotation>>> entry : sentenceToConceptMap.entrySet()) {
			TextAnnotation sentenceAnnot = entry.getKey();
			String keywordInSentence = sentenceContainsKeyword(sentenceAnnot.getCoveredText(), keywords);
			if (keywords == null || keywords.isEmpty() || keywordInSentence != null) {
				if (entry.getValue().size() > 1) {
					// then this sentence contains at least 1 concept X and 1 concept Y
					Set<TextAnnotation> xConceptsInSentence = entry.getValue().get(X_CONCEPTS);
					Set<TextAnnotation> yConceptsInSentence = entry.getValue().get(Y_CONCEPTS);

					// for each pair of X&Y concepts, create an ExtractedSentence
					for (TextAnnotation xAnnot : xConceptsInSentence) {
						for (TextAnnotation yAnnot : yConceptsInSentence) {
							String xId = xAnnot.getClassMention().getMentionName();
							String xText = xAnnot.getCoveredText();
							List<Span> xSpan = offsetSpan(xAnnot.getSpans(), sentenceAnnot.getAnnotationSpanStart());
							String yId = yAnnot.getClassMention().getMentionName();
							String yText = yAnnot.getCoveredText();
							List<Span> ySpan = offsetSpan(yAnnot.getSpans(), sentenceAnnot.getAnnotationSpanStart());

							/**
							 * There are cases, e.g. extension classes, where the same ontology concepts
							 * exists in different ontologies. Extracted sentences should not link the same
							 * concept, e.g. concept x should not equal concept y.
							 */
							if (!(xId.equals(yId) || xSpan.equals(ySpan))) {
								ExtractedSentence es = new ExtractedSentence(documentId, xId, xText, xSpan,
										xPlaceholder, yId, yText, ySpan, yPlaceholder, keywordInSentence,
										documentText.substring(sentenceAnnot.getAnnotationSpanStart(), sentenceAnnot.getAnnotationSpanEnd()), documentText);
								extractedSentences.add(es);
							}
						}
					}
				}
			}
		}
		return extractedSentences;
	}

	/**
	 * return the input spans offset by the specified value. Useful for converting
	 * annotation spans relative to the document into spans relative to the
	 * sentence.
	 * 
	 * @param spans
	 * @param offset
	 * @return
	 */
	public static List<Span> offsetSpan(List<Span> spans, int offset) {
		List<Span> offsetSpans = new ArrayList<Span>();

		for (Span span : spans) {
			offsetSpans.add(new Span(span.getSpanStart() - offset, span.getSpanEnd() - offset));
		}

		return offsetSpans;
	}

	@VisibleForTesting
	protected static Map<TextAnnotation, Map<String, Set<TextAnnotation>>> buildSentenceToConceptMap(
			Collection<TextAnnotation> sentenceAnnots, List<TextAnnotation> conceptXAnnots,
			List<TextAnnotation> conceptYAnnots) {

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> map = new HashMap<TextAnnotation, Map<String, Set<TextAnnotation>>>();

		List<TextAnnotation> sentenceAnnotList = new ArrayList<TextAnnotation>(sentenceAnnots);
		Collections.sort(sentenceAnnotList, TextAnnotation.BY_SPAN());

		matchConceptsToSentence(map, sentenceAnnotList, conceptXAnnots, X_CONCEPTS);
		matchConceptsToSentence(map, sentenceAnnotList, conceptYAnnots, Y_CONCEPTS);

		return map;

	}

	private static void matchConceptsToSentence(Map<TextAnnotation, Map<String, Set<TextAnnotation>>> map,
			List<TextAnnotation> sentenceAnnots, List<TextAnnotation> conceptXAnnots, String conceptKey) {
		for (TextAnnotation xAnnot : conceptXAnnots) {
			for (TextAnnotation sentence : sentenceAnnots) {
				if (xAnnot.overlaps(sentence)) {
					if (map.containsKey(sentence)) {
						Map<String, Set<TextAnnotation>> innerMap = map.get(sentence);
						if (innerMap.containsKey(conceptKey)) {
							innerMap.get(conceptKey).add(xAnnot);
						} else {
							innerMap.put(conceptKey, CollectionsUtil.createSet(xAnnot));
						}
					} else {
						Map<String, Set<TextAnnotation>> innerMap = new HashMap<String, Set<TextAnnotation>>();
						innerMap.put(conceptKey, CollectionsUtil.createSet(xAnnot));
						map.put(sentence, innerMap);
					}
					break;
				}
			}
		}
	}

	@VisibleForTesting
	protected static String sentenceContainsKeyword(String sentence, Set<String> keywords) {
		if (keywords == null || keywords.isEmpty()) {
			return null;
		}
		for (String keyword : keywords) {
			Pattern p = Pattern.compile(String.format("\\b%s\\b", RegExPatterns.escapeCharacterForRegEx(keyword)),
					Pattern.CASE_INSENSITIVE);
			if (p.matcher(sentence).find()) {
				return keyword;
			}
		}
		return null;
	}

}
