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

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.CrfOrConcept;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConceptCooccurrenceCountsFn extends DoFn<KV<String, String>, KV<String, String>> {

	public enum CooccurLevel {
		DOCUMENT(DOCUMENT_ID_TO_CONCEPT_ID), SENTENCE(SENTENCE_ID_TO_CONCEPT_ID), TITLE(TITLE_ID_TO_CONCEPT_ID),
		ABSTRACT(ABSTRACT_ID_TO_CONCEPT_ID);

		// , PARAGRAPH -- will require code to make sure all text is covered by a
		// paragraph in full text docs

		private final TupleTag<String> outputTag;

		private CooccurLevel(TupleTag<String> outputTag) {
			this.outputTag = outputTag;
		}

		public TupleTag<String> getOutputTag() {
			return this.outputTag;
		}

	}

	private static final long serialVersionUID = 1L;

	public static Delimiter OUTPUT_FILE_COLUMN_DELIMITER = Delimiter.TAB;
	public static Delimiter OUTPUT_FILE_SET_DELIMITER = Delimiter.PIPE;

	@SuppressWarnings("serial")
	public static TupleTag<String> DOCUMENT_ID_TO_CONCEPT_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> ABSTRACT_ID_TO_CONCEPT_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> TITLE_ID_TO_CONCEPT_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> SENTENCE_ID_TO_CONCEPT_ID = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple computeCounts(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Set<CooccurLevel> levels, DocumentType docTypeToCount) {

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

							for (CooccurLevel level : levels) {

								Map<String, Set<String>> docIdToConceptIdMap = countConcepts(docId,
										statusEntityToText.getValue(), level, docTypeToCount);

								for (Entry<String, Set<String>> entry : docIdToConceptIdMap.entrySet()) {
									String textId = entry.getKey();
									Set<String> conceptIds = entry.getValue();
									String outStr = textId + OUTPUT_FILE_COLUMN_DELIMITER.delimiter() + CollectionsUtil
											.createDelimitedString(conceptIds, OUTPUT_FILE_SET_DELIMITER.delimiter());
									context.output(level.getOutputTag(), outStr);
								}

							}
						} catch (Throwable t) {
							EtlFailureData failure = PipelineMain.initFailure("Failure while counting concepts.",
									outputDocCriteria, timestamp, docId, t);
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

				}).withOutputTags(DOCUMENT_ID_TO_CONCEPT_ID, TupleTagList.of(ETL_FAILURE_TAG)
						.and(ABSTRACT_ID_TO_CONCEPT_ID).and(SENTENCE_ID_TO_CONCEPT_ID).and(TITLE_ID_TO_CONCEPT_ID)));
	}

	@VisibleForTesting
	protected static Map<String, Set<String>> countConcepts(String documentId, Map<DocumentCriteria, String> docs,
			CooccurLevel level, DocumentType docTypeToCount) throws IOException {

		Map<String, Set<String>> textIdToConceptIds = new HashMap<String, Set<String>>();

		String documentText = PipelineMain.getDocumentText(docs);
		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docs);

		// levelAnnots is a list of annotations at the specified level, so if level ==
		// document then it's a single annotation representing the entire document.If
		// level == sentence, then it's the list of all sentences found in the document.
		List<TextAnnotation> levelAnnots = getLevelAnnotations(documentId, level, documentText, docTypeToContentMap);

		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(docTypeToCount);

		conceptAnnots = removeAnnotsWithMissingTypes(conceptAnnots);

		Map<TextAnnotation, Set<TextAnnotation>> levelAnnotToConceptAnnotMap = matchConceptsToLevelAnnots(levelAnnots,
				conceptAnnots);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : levelAnnotToConceptAnnotMap.entrySet()) {
			TextAnnotation levelAnnot = entry.getKey();
			Set<TextAnnotation> conceptAnnotsInLevel = entry.getValue();

			String textId = computeUniqueTextIdentifier(documentId, level, levelAnnot);
			Set<String> uniqueConceptsInLevelAnnot = getUniqueConceptIds(conceptAnnotsInLevel);
			if (!uniqueConceptsInLevelAnnot.isEmpty()) {
				textIdToConceptIds.put(textId, uniqueConceptsInLevelAnnot);
			}
		}

		return textIdToConceptIds;
	}

	protected static String computeUniqueTextIdentifier(String documentId, CooccurLevel level,
			TextAnnotation levelAnnot) {
		// if the level is DOCUMENT then there isn't a need to create longer identifiers
		if (level == CooccurLevel.DOCUMENT) {
			return documentId;
		}
		return documentId + "_" + level.name().toLowerCase() + "_" + DigestUtils
				.sha256Hex(levelAnnot.getAggregateSpan().toString() + levelAnnot.getCoveredText().substring(0, 8));
	}

	/**
	 * The output contains a blank identifiers, so we remove any annotations with
	 * missing types here
	 * 
	 * @param conceptAnnots
	 * @return
	 */
	protected static Collection<TextAnnotation> removeAnnotsWithMissingTypes(Collection<TextAnnotation> conceptAnnots) {
		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
		for (TextAnnotation annot : conceptAnnots) {
			String type = annot.getClassMention().getMentionName();
			if (!type.trim().isEmpty()) {
				toKeep.add(annot);
			}
		}
		return toKeep;
	}

	/**
	 * Given a list of concept annotations and a list of cooccurrence-level
	 * annotations (sentences, paragraphs, etc.), return a mapping from level
	 * annotations to concept annotations they contain.
	 * 
	 * @param levelAnnots
	 * @param conceptAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static Map<TextAnnotation, Set<TextAnnotation>> matchConceptsToLevelAnnots(
			Collection<TextAnnotation> levelAnnots, Collection<TextAnnotation> conceptAnnots) {
		Map<TextAnnotation, Set<TextAnnotation>> map = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		// annotations must be in sorted lists so we can break the loops below
		// efficiently
		List<TextAnnotation> levelAnnotList = new ArrayList<TextAnnotation>();
		for (TextAnnotation ta : levelAnnots) {
			levelAnnotList.add(PipelineMain.clone(ta));
		}
		List<TextAnnotation> conceptAnnotList = new ArrayList<TextAnnotation>();
		for (TextAnnotation ta : conceptAnnots) {
			conceptAnnotList.add(PipelineMain.clone(ta));
		}

		Collections.sort(levelAnnotList, TextAnnotation.BY_SPAN());
		Collections.sort(conceptAnnotList, TextAnnotation.BY_SPAN());

		for (TextAnnotation concept : conceptAnnotList) {
			for (TextAnnotation level : levelAnnotList) {
				if (concept.overlaps(level) && level.getAggregateSpan().containsSpan(concept.getAggregateSpan())) {
					if (map.containsKey(level)) {
						map.get(level).add(concept);
					} else {
						Set<TextAnnotation> concepts = new HashSet<TextAnnotation>();
						concepts.add(concept);
						map.put(level, concepts);
					}
					break;
				}
			}
		}
		return map;
	}

	private static Set<String> getUniqueConceptIds(Collection<TextAnnotation> conceptAnnots) {
		Set<String> conceptIds = new HashSet<String>();
		for (TextAnnotation annot : conceptAnnots) {
			conceptIds.add(annot.getClassMention().getMentionName());
		}
		return conceptIds;
	}

	/**
	 * Depending on the requested cooccurrence Level, return the appropriate level
	 * annotations. If the level is Document, then return one annotation that spans
	 * the entire document. If the level is Sentence, then the
	 * sentenceAnnotationBionlp variable should contain the sentence annotations in
	 * bionlp format, and the sentence annotations should be returned.
	 * 
	 * @param documentId
	 * @param level
	 * @param documentText
	 * @param sentenceAnnotationBionlp
	 * @param sectionAnnotationBionlp
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static List<TextAnnotation> getLevelAnnotations(String documentId, CooccurLevel level,
			String documentText, Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap) throws IOException {
		List<TextAnnotation> levelAnnots = new ArrayList<TextAnnotation>();

		switch (level) {
		case DOCUMENT:
			TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(documentId);
			levelAnnots.add(factory.createAnnotation(0, documentText.length(), documentText, "document"));
			break;
		case SENTENCE:
			if (docTypeToContentMap.containsKey(DocumentType.SENTENCE)) {
				levelAnnots.addAll(docTypeToContentMap.get(DocumentType.SENTENCE));
			} else {
				throw new IllegalArgumentException(
						"Cooccurrence level = SENTENCE, however there are no sentence annotations in the input for document "
								+ documentId);
			}
			break;
		case TITLE:
			if (docTypeToContentMap.containsKey(DocumentType.SECTIONS)) {
				for (TextAnnotation annot : docTypeToContentMap.get(DocumentType.SECTIONS)) {
					String sectionType = annot.getClassMention().getMentionName();
					if (sectionType.equalsIgnoreCase("title")) {
						levelAnnots.add(annot);
						break;
					}
				}
			} else {
				throw new IllegalArgumentException(
						"Cooccurrence level = TITLE, however there are no section annotations in the input for document "
								+ documentId);
			}
			break;

		case ABSTRACT:
			if (docTypeToContentMap.containsKey(DocumentType.SECTIONS)) {
				for (TextAnnotation annot : docTypeToContentMap.get(DocumentType.SECTIONS)) {
					String sectionType = annot.getClassMention().getMentionName();
					if (sectionType.equalsIgnoreCase("abstract")) {
						levelAnnots.add(annot);
						break;
					}
				}
			} else {
				throw new IllegalArgumentException(
						"Cooccurrence level = ABSTRACT, however there are no section annotations in the input for document "
								+ documentId);
			}
			break;

		default:
			throw new IllegalArgumentException(
					String.format("Unhandled level type (%s). Code changes required.", level.name()));
		}
		return levelAnnots;
	}

	/**
	 * Add a document of a given type to the map
	 * 
	 * @param conceptTypeToContentMap
	 * @param documentContent
	 * @param type
	 * @param crfOrConcept
	 */
	@VisibleForTesting
	protected static void addToMap(Map<String, Map<CrfOrConcept, String>> conceptTypeToContentMap,
			String documentContent, String type, CrfOrConcept crfOrConcept) {
		if (conceptTypeToContentMap.containsKey(type)) {
			conceptTypeToContentMap.get(type).put(crfOrConcept, documentContent);
		} else {
			Map<CrfOrConcept, String> innerMap = new HashMap<CrfOrConcept, String>();
			innerMap.put(crfOrConcept, documentContent);
			conceptTypeToContentMap.put(type, innerMap);
		}
	}

	@Data
	public static class ConceptPair extends DoFn {
		private static final long serialVersionUID = 1L;
		private final String conceptId1;
		private final String conceptId2;

		public ConceptPair(String conceptId1, String conceptId2) {
			if (conceptId1.compareTo(conceptId2) < 1) {
				this.conceptId1 = conceptId1;
				this.conceptId2 = conceptId2;
			} else {
				this.conceptId1 = conceptId2;
				this.conceptId2 = conceptId1;
			}
		}

		public String toReproducibleKey() {
			return (conceptId1.compareTo(conceptId2) < 1) ? conceptId1 + "|" + conceptId2
					: conceptId2 + "|" + conceptId1;
		}

		public static ConceptPair fromReproducibleKey(String s) {
			String[] cols = s.split("\\|");
			return new ConceptPair(cols[0], cols[1]);
		}

		public String getPairId() {
			return DigestUtil.getBase64Sha1Digest(toReproducibleKey());
		}

//		@Override
//		public boolean equals(Object obj) {
//			if (this == obj)
//				return true;
//			if (obj == null)
//				return false;
//			if (getClass() != obj.getClass())
//				return false;
//			ConceptPair other = (ConceptPair) obj;
//
//			Set<String> set = CollectionsUtil.createSet(conceptId1, conceptId2);
//			Set<String> otherSet = CollectionsUtil.createSet(other.getConceptId1(), other.getConceptId2());
//
//			return set.equals(otherSet);
//		}
//
//		@Override
//		public int hashCode() {
//			Set<String> set = CollectionsUtil.createSet(conceptId1, conceptId2);
//			return set.hashCode();
//		}
//
//		@Override
//		public String toString() {
//			return String.format("[%s, %s]", conceptId1, conceptId2);
//		}

	}

	/**
	 * Data structure to store normalized google distance scores
	 */
	@Data
	@SuppressWarnings("rawtypes")
	@EqualsAndHashCode(callSuper = false)
	public static class NGDScore extends DoFn {
		private static final long serialVersionUID = 1L;
		private final ConceptPair pair;
		private final double score;
	}
}
