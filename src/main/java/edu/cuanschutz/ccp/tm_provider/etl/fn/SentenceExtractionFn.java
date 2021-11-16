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
import org.apache.beam.sdk.values.PCollectionView;
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
public class SentenceExtractionFn extends DoFn<KV<String, String>, KV<String, String>> {

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

	/**
	 * @param statusEntityToText
	 * @param keywords
	 * @param outputDocCriteria
	 * @param timestamp
	 * @param requiredDocumentCriteria
	 * @param prefixToPlaceholderMap
	 * @param conceptDocType           either DocumentType.CONCEPT_ALL or
	 *                                 DocumentType.CONCEPT_ALL_UNFILTERED
	 * @return
	 */
	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText, Set<String> keywords,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Map<List<String>, String> prefixesToPlaceholderMap,
			DocumentType conceptDocType, PCollectionView<Map<String, Set<String>>> ancestorsMapView,
			Set<String> conceptIdsToExclude) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, ExtractedSentence>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						Map<String, Set<String>> ancestorsMap = context.sideInput(ancestorsMapView);

						Set<String> documentPublicationTypes = Collections.emptySet();
						List<String> publicationTypes = statusEntity.getPublicationTypes();
						if (publicationTypes != null) {
							documentPublicationTypes = new HashSet<String>(publicationTypes);
						}
						String yearPublished = statusEntity.getYearPublished();
						// 2155 is the max year value in MySQL
						int documentYearPublished = 2155;
						if (yearPublished != null && !yearPublished.isEmpty()) {
							documentYearPublished = Integer.parseInt(yearPublished);
						}

						try {
							String documentText = PipelineMain.getDocumentText(statusEntityToText.getValue());

							Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
									.getDocTypeToContentMap(docId, statusEntityToText.getValue());

							Set<ExtractedSentence> extractedSentences = extractSentences(docId, documentText,
									documentPublicationTypes, documentYearPublished, docTypeToContentMap, keywords,
									prefixesToPlaceholderMap, conceptDocType, ancestorsMap, conceptIdsToExclude);
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

				}).withSideInputs(ancestorsMapView)
				.withOutputTags(EXTRACTED_SENTENCES_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	@VisibleForTesting
	protected static Set<ExtractedSentence> extractSentences(String documentId, String documentText,
			Set<String> documentPublicationTypes, int documentYearPublished,
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap, Set<String> keywords,
			Map<List<String>, String> prefixesToPlaceholderMap, DocumentType conceptDocType,
			Map<String, Set<String>> ancestorMap, Set<String> conceptIdsToExclude) throws IOException {

		Collection<TextAnnotation> sentenceAnnots = docTypeToContentMap.get(DocumentType.SENTENCE);
		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(conceptDocType);
		Collection<TextAnnotation> sectionAnnots = docTypeToContentMap.get(DocumentType.SECTIONS);

		// remove concepts-to-exclude
		conceptAnnots = removeConceptsToExclude(conceptAnnots, conceptIdsToExclude);

		List<String> xPrefixes = new ArrayList<String>();
		List<String> yPrefixes = new ArrayList<String>();
		String xPlaceholder = null;
		// there are only two placeholders in the map, e.g. @GENE$ so we use the
		// placeholders to assign the prefixes to distinct lists
		for (List<String> prefixes : prefixesToPlaceholderMap.keySet()) {
			if (xPlaceholder == null || xPlaceholder.equals(prefixesToPlaceholderMap.get(prefixes))) {
				xPrefixes.addAll(prefixes);
				xPlaceholder = prefixesToPlaceholderMap.get(prefixes);
			} else {
				yPrefixes.addAll(prefixes);
			}
		}

		// if there is only one prefix - then we are looking for sentences that have two
		// of the same kind of anntotation, e.g. two protein annotations
		if (yPrefixes.isEmpty()) {
			yPrefixes.addAll(xPrefixes);
		}

		List<TextAnnotation> conceptXAnnots = getAnnotsByPrefix(conceptAnnots, xPrefixes, ancestorMap);
		List<TextAnnotation> conceptYAnnots = getAnnotsByPrefix(conceptAnnots, yPrefixes, ancestorMap);
		
		Set<ExtractedSentence> extractedSentences = new HashSet<ExtractedSentence>();
		if (!conceptXAnnots.isEmpty() && !conceptYAnnots.isEmpty()) {

			Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = buildSentenceToConceptMap(
					sentenceAnnots, conceptXAnnots, conceptYAnnots);

			// xPlaceholder was already set above
//			String xPlaceholder = prefixToPlaceholderMap.get(xPrefixes.get(0));
			String yPlaceholder = prefixesToPlaceholderMap.get(yPrefixes);

			extractedSentences
					.addAll(catalogExtractedSentences(keywords, documentText, documentId, documentPublicationTypes,
							documentYearPublished, sentenceToConceptMap, xPlaceholder, yPlaceholder, sectionAnnots));

		}
		return extractedSentences;
	}

	/**
	 * @param conceptAnnots
	 * @param conceptIdsToExclude
	 * @return collection of TextAnnotations, excluding any that reference concept
	 *         CURIEs in the conceptIdsToExclude set
	 */
	private static Collection<TextAnnotation> removeConceptsToExclude(Collection<TextAnnotation> conceptAnnots,
			Set<String> conceptIdsToExclude) {
		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
		for (TextAnnotation annot : conceptAnnots) {
			String id = annot.getClassMention().getMentionName();
			if (!conceptIdsToExclude.contains(id)) {
				toKeep.add(annot);
			}
		}

		return toKeep;
	}

	/**
	 * filters the input collection of annotations by keeping only those that share
	 * the specified prefix, e.g. CHEBI
	 * 
	 * Note: the prefix can now be just a prefix, e.g. "GO" or it can be a CURIE,
	 * e.g. "GO:0005575" to indicate a subset of an ontology - in this case the
	 * GO:cellular_component hierarchy. The ancestor map is used to ensure that any
	 * concept identifier that starts with "GO:" is also a descendant of
	 * "GO:0005575".
	 * 
	 * @param conceptAnnots
	 * @param xPrefix
	 * @return
	 */
	public static List<TextAnnotation> getAnnotsByPrefix(Collection<TextAnnotation> conceptAnnots,
			List<String> prefixes, Map<String, Set<String>> ancestorMap) {
		Set<TextAnnotation> annots = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : conceptAnnots) {
			for (String prefixOrAncestorId : prefixes) {
				String prefix = prefixOrAncestorId;
				String ancestorId = null;
				if (prefixOrAncestorId.contains(":")) {
					prefix = prefixOrAncestorId.substring(0, prefixOrAncestorId.indexOf(":"));
					ancestorId = prefixOrAncestorId;
				}

				String conceptId = annot.getClassMention().getMentionName();
				if (ancestorId == null) {
					// then the prefixOrAncestorId is simply a prefix, so we look to see if the
					// concept ID also starts with that prefix
					if (conceptId.startsWith(prefix)) {
						annots.add(annot);
					}
				} else {
					// the prefixOrAncestorId is an ancestor ID so we make sure the concept ID
					// exists in the ancestor map and that the ancestor ID is an ancestor of the
					// concept ID.
					if (ancestorMap.containsKey(conceptId) && ancestorMap.get(conceptId).contains(ancestorId)
							|| conceptId.equals(ancestorId)) {
						annots.add(annot);
					}
				}
			}
		}

		List<TextAnnotation> annotList = new ArrayList<TextAnnotation>(annots);
		Collections.sort(annotList, TextAnnotation.BY_SPAN());
		return annotList;
	}

	@VisibleForTesting
	protected static Set<ExtractedSentence> catalogExtractedSentences(Set<String> keywords, String documentText,
			String documentId, Set<String> documentPublicationTypes, int documentYearPublished,
			Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap, String xPlaceholder,
			String yPlaceholder, Collection<TextAnnotation> sectionAnnots) {

		Set<ExtractedSentence> extractedSentences = new HashSet<ExtractedSentence>();
		for (Entry<TextAnnotation, Map<String, Set<TextAnnotation>>> entry : sentenceToConceptMap.entrySet()) {
			TextAnnotation sentenceAnnot = entry.getKey();
			String documentZone = determineDocumentZone(sentenceAnnot, sectionAnnots);
			String keywordInSentence = sentenceContainsKeyword(sentenceAnnot.getCoveredText(), keywords);
			if (keywords == null || keywords.isEmpty() || keywordInSentence != null) {
				if (entry.getValue().size() > 1) {
					// then this sentence contains at least 1 concept X and 1 concept Y
					Set<TextAnnotation> xConceptsInSentence = entry.getValue().get(X_CONCEPTS);
					Set<TextAnnotation> yConceptsInSentence = entry.getValue().get(Y_CONCEPTS);

					// if there are X concepts that share a span, then we will combine the
					// identifiers for these concepts into a single TextAnnotation object; similar
					// for Y concepts
					xConceptsInSentence = mergeOverlappingConcepts(xConceptsInSentence);
					yConceptsInSentence = mergeOverlappingConcepts(yConceptsInSentence);

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
										documentText.substring(sentenceAnnot.getAnnotationSpanStart(),
												sentenceAnnot.getAnnotationSpanEnd()),
//										documentText,
										documentZone, documentPublicationTypes, documentYearPublished);
								extractedSentences.add(es);
							}
						}
					}
				}
			}
		}
		return extractedSentences;
	}

	private static Set<TextAnnotation> mergeOverlappingConcepts(Set<TextAnnotation> conceptAnnots) {
		Map<List<Span>, TextAnnotation> spanToAnnotMap = new HashMap<List<Span>, TextAnnotation>();

		for (TextAnnotation annot : conceptAnnots) {
			List<Span> spans = annot.getSpans();
			if (spanToAnnotMap.containsKey(spans)) {
				String updatedID = spanToAnnotMap.get(spans).getClassMention().getMentionName() + "|"
						+ annot.getClassMention().getMentionName();
				spanToAnnotMap.get(spans).getClassMention().setMentionName(updatedID);
			} else {
				spanToAnnotMap.put(spans, annot);
			}
		}

		return new HashSet<TextAnnotation>(spanToAnnotMap.values());

	}

	/**
	 * 
	 * @param sentenceAnnot
	 * @return
	 */
	protected static String determineDocumentZone(TextAnnotation sentenceAnnot,
			Collection<TextAnnotation> sectionAnnots) {

		List<TextAnnotation> sortedSectionAnnots = new ArrayList<TextAnnotation>(sectionAnnots);
		Collections.sort(sortedSectionAnnots, TextAnnotation.BY_SPAN());

		for (TextAnnotation section : sortedSectionAnnots) {
			if (sentenceAnnot.overlaps(section)) {
				return section.getClassMention().getMentionName();
			}
		}

		return "Unknown";
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
