package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.tools.ant.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.string.RegExPatterns;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
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

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText, Set<String> keywords,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Map<String, String> suffixToPlaceholderMap) {

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
							Set<ExtractedSentence> extractedSentences = extractSentences(requiredDocumentCriteria,
									statusEntityToText, keywords, suffixToPlaceholderMap);
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
	protected static Set<ExtractedSentence> extractSentences(Set<DocumentCriteria> requiredDocCriteria,
			KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText, Set<String> keywords,
			Map<String, String> suffixToPlaceholderMap) throws IOException {
		// check to see that all required documents are present -- it's possible that
		// due to various processing errors that one or more of the documents may be
		// missing.
		Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
		if (!docs.keySet().equals(requiredDocCriteria)) {

//			throw new IllegalArgumentException("MISSING DOCS(" + statusEntityToText.getKey().getDocumentId()
//					+ "). HAS: " + docs.keySet().toString() + " BUT NEEDED: " + requiredDocCriteria.toString());

			return null;
		}

		String documentText = null;
		String sentenceAnnotationBionlp = null;
		String conceptAnnotationBionlpX = null;
		String crfAnnotationBionlpX = null;
		String conceptAnnotationBionlpY = null;
		String crfAnnotationBionlpY = null;

		String xSuffix = null;
		String ySuffix = null;

		for (Entry<DocumentCriteria, String> entry : docs.entrySet()) {
			DocumentType documentType = entry.getKey().getDocumentType();
			if (documentType == DocumentType.TEXT) {
				documentText = entry.getValue();
			} else if (documentType == DocumentType.SENTENCE) {
				sentenceAnnotationBionlp = entry.getValue();
			} else if (documentType.name().startsWith("CRF_")) {
				if (xSuffix == null) {
					// then we haven't assigned the X concept
					xSuffix = StringUtils.removePrefix(documentType.name(), "CRF_");
					crfAnnotationBionlpX = entry.getValue();
				} else {
					// X concept has been assigned, so we need to check to see if this is X
					if (documentType.name().endsWith(xSuffix)) {
						crfAnnotationBionlpX = entry.getValue();
					} else {
						ySuffix = StringUtils.removePrefix(documentType.name(), "CRF_");
						crfAnnotationBionlpY = entry.getValue();
					}
				}
			} else if (documentType.name().startsWith("CONCEPT_")) {
				if (xSuffix == null) {
					// then we haven't assigned the X concept
					xSuffix = StringUtils.removePrefix(documentType.name(), "CONCEPT_");
					conceptAnnotationBionlpX = entry.getValue();
				} else {
					// X concept has been assigned, so we need to check to see if this is X
					if (documentType.name().endsWith(xSuffix)) {
						conceptAnnotationBionlpX = entry.getValue();
					} else {
						conceptAnnotationBionlpY = entry.getValue();
					}
				}
			}
		}

		// at this point all 6 variables above should have been assigned
		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		String documentId = statusEntityToText.getKey().getDocumentId();
		TextDocument sentenceDocument = reader.readDocument(documentId, "unknown",
				new ByteArrayInputStream(sentenceAnnotationBionlp.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextDocument conceptXDocument = reader.readDocument(documentId, "unknown",
				new ByteArrayInputStream(conceptAnnotationBionlpX.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextDocument conceptYDocument = reader.readDocument(documentId, "unknown",
				new ByteArrayInputStream(conceptAnnotationBionlpY.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextDocument crfXDocument = reader.readDocument(documentId, "unknown",
				new ByteArrayInputStream(crfAnnotationBionlpX.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextDocument crfYDocument = reader.readDocument(documentId, "unknown",
				new ByteArrayInputStream(crfAnnotationBionlpY.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		List<TextAnnotation> conceptXAnnots = PipelineMain.filterViaCrf(conceptXDocument.getAnnotations(),
				crfXDocument.getAnnotations());

		Set<ExtractedSentence> extractedSentences = new HashSet<ExtractedSentence>();
		if (!conceptXAnnots.isEmpty()) {
			List<TextAnnotation> conceptYAnnots = PipelineMain.filterViaCrf(conceptYDocument.getAnnotations(),
					crfYDocument.getAnnotations());
			if (!conceptYAnnots.isEmpty()) {

				Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = buildSentenceToConceptMap(
						sentenceDocument.getAnnotations(), conceptXAnnots, conceptYAnnots);

				String xPlaceholder = suffixToPlaceholderMap.get(xSuffix);
				String yPlaceholder = suffixToPlaceholderMap.get(ySuffix);

				extractedSentences.addAll(catalogExtractedSentences(keywords, documentText, documentId,
						sentenceToConceptMap, xPlaceholder, yPlaceholder));

			}
		}
		return extractedSentences;
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
										sentenceAnnot.getCoveredText(), documentText);
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
	private static List<Span> offsetSpan(List<Span> spans, int offset) {
		List<Span> offsetSpans = new ArrayList<Span>();

		for (Span span : spans) {
			offsetSpans.add(new Span(span.getSpanStart() - offset, span.getSpanEnd() - offset));
		}

		return offsetSpans;
	}

	@VisibleForTesting
	protected static Map<TextAnnotation, Map<String, Set<TextAnnotation>>> buildSentenceToConceptMap(
			List<TextAnnotation> sentenceAnnots, List<TextAnnotation> conceptXAnnots,
			List<TextAnnotation> conceptYAnnots) {

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> map = new HashMap<TextAnnotation, Map<String, Set<TextAnnotation>>>();

		Collections.sort(sentenceAnnots, TextAnnotation.BY_SPAN());

		matchConceptsToSentence(map, sentenceAnnots, conceptXAnnots, X_CONCEPTS);
		matchConceptsToSentence(map, sentenceAnnots, conceptYAnnots, Y_CONCEPTS);

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
