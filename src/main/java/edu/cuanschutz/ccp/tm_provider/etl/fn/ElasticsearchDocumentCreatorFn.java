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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ElasticsearchDocumentCreatorFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<String> ELASTICSEARCH_DOCUMENT_JSON_TAG = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple createDocuments(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			com.google.cloud.Timestamp timestamp, Set<DocumentCriteria> requiredDocumentCriteria,
			DocumentCriteria errorCriteria, DocumentType conceptDocumentType) {

		return statusEntityToText.apply("Identify concept annotations",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();

						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();
						try {

							// If the loaded document types don't match the required types, then throw an
							// exception which will get logged as a failure in datastore
							validatePresenceOfRequiredDocuments(docId, statusEntityToText.getValue(),
									requiredDocumentCriteria);

							Set<String> elasticsearchSentenceJsonDocuments = createSentenceJsonDocuments(docId,
									statusEntityToText.getValue(), conceptDocumentType);

							for (String jsonDoc : elasticsearchSentenceJsonDocuments) {
								context.output(ELASTICSEARCH_DOCUMENT_JSON_TAG, jsonDoc);
							}

						} catch (Throwable t) {
							EtlFailureData failure = PipelineMain.initFailure(
									"Failure while creating elasticsearch JSON sentence documents.", errorCriteria,
									timestamp, docId, t);
							context.output(ETL_FAILURE_TAG, failure);
						}

					}

					/**
					 * If the loaded document types don't match the required types, then throw an
					 * exception
					 * 
					 * @param documentId
					 * @param docs
					 * @param requiredDocumentCriteria
					 */
					private void validatePresenceOfRequiredDocuments(String documentId,
							Map<DocumentCriteria, String> docs, Set<DocumentCriteria> requiredDocumentCriteria) {

						for (DocumentCriteria requiredCriteria : requiredDocumentCriteria) {
							if (!docs.keySet().contains(requiredCriteria)) {
								throw new IllegalArgumentException(
										"Invalid document input (" + documentId + "). HAS: " + docs.keySet().toString()
												+ " BUT NEEDED: " + requiredDocumentCriteria.toString());
							}
						}

					}

				}).withOutputTags(ELASTICSEARCH_DOCUMENT_JSON_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * @param documentId
	 * @param docs
	 * @param conceptDocumentType - CONCEPT_ALL or CONCEPT_FILTERED
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static Set<String> createSentenceJsonDocuments(String documentId, Map<DocumentCriteria, String> docs,
			DocumentType conceptDocumentType) throws IOException {

		Set<String> jsonDocuments = new HashSet<String>();

		String documentText = PipelineMain.getDocumentText(docs);
		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docs);

		// levelAnnots is a list of annotations at the specified level, so if level ==
		// document then it's a single annotation representing the entire document.If
		// level == sentence, then it's the list of all sentences found in the document.
		List<TextAnnotation> sentenceAnnots = ConceptCooccurrenceCountsFn.getLevelAnnotations(documentId,
				CooccurLevel.SENTENCE, documentText, docTypeToContentMap);

		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(conceptDocumentType);

		conceptAnnots = ConceptCooccurrenceCountsFn.removeAnnotsWithMissingTypes(conceptAnnots);

		Map<TextAnnotation, Set<TextAnnotation>> sentenceAnnotToConceptAnnotMap = ConceptCooccurrenceCountsFn
				.matchConceptsToLevelAnnots(sentenceAnnots, conceptAnnots);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : sentenceAnnotToConceptAnnotMap.entrySet()) {
			TextAnnotation sentenceAnnot = entry.getKey();
			Set<TextAnnotation> conceptAnnotsInSentence = entry.getValue();

			String sentenceId = computeSentenceIdentifier(documentText, sentenceAnnot);

			String elasticsearchSentenceDocumentJson = createJsonDocument(sentenceId, sentenceAnnot,
					conceptAnnotsInSentence, documentId, documentText);

			jsonDocuments.add(elasticsearchSentenceDocumentJson);
		}

		return jsonDocuments;
	}

	@VisibleForTesting
	protected static String createJsonDocument(String sentenceId, TextAnnotation sentenceAnnot,
			Set<TextAnnotation> conceptAnnotsInSentence, String documentId, String documentText) { // , String documentSection, int
																				// publicationYear) {
		Sentence sentence = Sentence.buildSentence(sentenceAnnot, conceptAnnotsInSentence, sentenceId, documentId, documentText);
//				documentSection, publicationYear);

		System.out.println("sentence id: " + sentence.getId());
		System.out.println("sentence doc id: " + sentence.getDocumentId());
		
		Gson gson = new Gson();
		String json = gson.toJson(sentence);

		// % sign must be URL encoded
		json = json.replaceAll("%", "%25");
		json = json.replaceAll("=", "%3D");

		return json;
	}

	/**
	 * This is a utility method to decode the characters that were encoded in the
	 * methods above and below.
	 * 
	 * @param encodedString
	 * @return
	 */
	public static String decode(String encodedString) {

		String decodedString = encodedString.replaceAll("%25", "%");
		decodedString = decodedString.replaceAll("%3D", "=");
		decodedString = decodedString.replaceAll("%29", ")");
		decodedString = decodedString.replaceAll("%28", "(");

		return decodedString;
	}

	protected static String computeSentenceIdentifier(String documentText, TextAnnotation annot) {
		String sentenceText = documentText.substring(annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd());
		return computeSentenceIdentifier(sentenceText);
	}
	
	public static String computeSentenceIdentifier(String sentenceText) {
		return DigestUtils.sha256Hex(sentenceText);
	}

	public static class Sentence {

		public static final String ID = "id";
		public static final String DOCUMENT_ID = "documentId";
		public static final String ANNOTATED_TEXT = "annotatedText";
		public static final String SECTION = "section";
		public static final String PUBLICATION_YEAR = "publicationYear";

		private String id;
		private String documentId;
		private String annotatedText;
//		private final String section;
//		private final int publicationYear;

//		use nested fields to store relations
//		label each concept with T1, T2, etc in the annotated-text field and in the relations so that they can be mapped to each other
//		
//		private final Set<String> subjects;
//		private final Set<String> predicates;
//		private final Set<String> objects;
//		
//		private final Set<String> subPred;
//		private final Set<String> predObj;
//		
//		private final boolean manuallyVetted;
		
		
		public Sentence(String id, String documentId, String annotatedText) {
			this.setId(id);
			this.setDocumentId(documentId);
			this.setAnnotatedText(annotatedText);
		}
		
		public Sentence() {
			
		}
		
		
		

		public static Sentence buildSentence(TextAnnotation sentenceAnnot, Set<TextAnnotation> conceptAnnots,
				String sentenceId, String documentId, String documentText) {// , String section, int publicationYear) {

			String annotatedText = getAnnotatedText(sentenceAnnot, conceptAnnots, documentText);

			return new Sentence(sentenceId, documentId, annotatedText);// , section, publicationYear);
		}

		@VisibleForTesting
		protected static String getAnnotatedText(TextAnnotation sentenceAnnot, Set<TextAnnotation> conceptAnnots, String documentText) {

			Set<TextAnnotation> unnestedConceptAnnotations = removeNestedAnnotations(conceptAnnots);

			unnestedConceptAnnotations = adjustForSentenceOffset(unnestedConceptAnnotations,
					sentenceAnnot.getAnnotationSpanStart());

			unnestedConceptAnnotations = replaceColonWithUnderscoreInIds(unnestedConceptAnnotations);

			/*
			 * NOTE: ontology identifiers use underscores instead of colons from this point
			 * forward
			 */

			Map<Span, Set<TextAnnotation>> spanToConceptAnnotMap = getSpanToAnnotMap(unnestedConceptAnnotations);

			Map<Span, Set<TextAnnotation>> sortedSpanToConceptAnnotMap = CollectionsUtil
					.sortMapByKeys(spanToConceptAnnotMap, SortOrder.ASCENDING);

//			String sentenceText = sentenceAnnot.getCoveredText();
			
			String sentenceText = documentText.substring(sentenceAnnot.getAnnotationSpanStart(), sentenceAnnot.getAnnotationSpanEnd());

			StringBuilder sb = new StringBuilder();

			int offset = 0;
			for (Entry<Span, Set<TextAnnotation>> entry : sortedSpanToConceptAnnotMap.entrySet()) {
				Span span = entry.getKey();
				Set<TextAnnotation> annots = entry.getValue();
				List<String> conceptIds = getSortedIds(annots);
				List<String> sortedOntologyPrefixes = getSortedOntologyPrefixes(conceptIds);

				conceptIds.addAll(sortedOntologyPrefixes);
				String idStr = CollectionsUtil.createDelimitedString(conceptIds, "&");

				String entityText = sentenceText.substring(span.getSpanStart(), span.getSpanEnd())
						.replaceAll("\\(", "%28").replaceAll("\\)", "%29");
				String formattedEntity = String.format("(%s)[%s]", entityText, idStr);

				String sentenceSubstringBefore = sentenceText.substring(offset, span.getSpanStart())
						.replaceAll("\\(", "%28").replaceAll("\\)", "%29");

				sb.append(sentenceSubstringBefore);
				sb.append(formattedEntity);
				offset = span.getSpanEnd();
			}

			// add the tail end of the sentence
			String sentenceTail = sentenceText.substring(offset).replaceAll("\\(", "%28").replaceAll("\\)", "%29");
			
			sb.append(sentenceTail);

			return sb.toString();
		}

		private static Set<TextAnnotation> replaceColonWithUnderscoreInIds(Set<TextAnnotation> annots) {
			Set<TextAnnotation> adjustedAnnots = new HashSet<TextAnnotation>();

			for (TextAnnotation annot : annots) {
				String updatedId = annot.getClassMention().getMentionName().replaceAll(":", "_");
				annot.getClassMention().setMentionName(updatedId);
				adjustedAnnots.add(annot);
			}

			return adjustedAnnots;
		}

		private static Set<TextAnnotation> adjustForSentenceOffset(Set<TextAnnotation> annots, int spanOffset) {
			Set<TextAnnotation> adjustedAnnots = new HashSet<TextAnnotation>();

			for (TextAnnotation annot : annots) {
				List<Span> adjustedSpans = new ArrayList<Span>();
				for (Span span : annot.getSpans()) {
					Span adjustedSpan = new Span(span.getSpanStart() - spanOffset, span.getSpanEnd() - spanOffset);
					adjustedSpans.add(adjustedSpan);
				}
				annot.setSpans(adjustedSpans);
				adjustedAnnots.add(annot);
			}

			return adjustedAnnots;
		}

		@VisibleForTesting
		protected static Set<TextAnnotation> removeNestedAnnotations(Set<TextAnnotation> annots) {
			Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();

			for (TextAnnotation annot : annots) {
				for (TextAnnotation annot2 : annots) {
					if (annot.getAggregateSpan().containsSpan(annot2.getAggregateSpan())
							&& !annot.getAggregateSpan().equals(annot2.getAggregateSpan())) {
						toRemove.add(annot2);
					} else if (annot.getAggregateSpan().overlaps(annot2.getAggregateSpan())
							&& !annot.getAggregateSpan().equals(annot2.getAggregateSpan())) {
						// keep the longer annotation
						if (annot.length() > annot2.length()) {
							toRemove.add(annot2);
						} else {
							toRemove.add(annot);
						}
					}
				}
			}

			Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>(annots);
			toKeep.removeAll(toRemove);
			return toKeep;
		}

		@VisibleForTesting
		protected static Map<Span, Set<TextAnnotation>> getSpanToAnnotMap(Collection<TextAnnotation> annots) {
			Map<Span, Set<TextAnnotation>> map = new HashMap<Span, Set<TextAnnotation>>();
			for (TextAnnotation annot : annots) {
				CollectionsUtil.addToOne2ManyUniqueMap(annot.getAggregateSpan(), annot, map);
			}
			return map;
		}

		@VisibleForTesting
		protected static List<String> getSortedIds(Set<TextAnnotation> annots) {
			Set<String> ids = new HashSet<String>();
			for (TextAnnotation annot : annots) {
				ids.add(annot.getClassMention().getMentionName());
			}
			List<String> sortedIds = new ArrayList<String>(ids);
			Collections.sort(sortedIds);
			return sortedIds;
		}

		@VisibleForTesting
		protected static List<String> getSortedOntologyPrefixes(List<String> conceptIds) {
			Set<String> prefixes = new HashSet<String>();
			for (String conceptId : conceptIds) {
				String prefix = conceptId.split("_")[0];
				prefixes.add("_" + prefix);
			}

			List<String> sortedPrefixes = new ArrayList<String>(prefixes);
			Collections.sort(sortedPrefixes);
			return sortedPrefixes;
		}




		public String getId() {
			return id;
		}




		public String getDocumentId() {
			return documentId;
		}




		public String getAnnotatedText() {
			return annotatedText;
		}




		public void setId(String id) {
			this.id = id;
		}




		public void setDocumentId(String documentId) {
			this.documentId = documentId;
		}




		public void setAnnotatedText(String annotatedText) {
			this.annotatedText = annotatedText;
		}

	}

}
