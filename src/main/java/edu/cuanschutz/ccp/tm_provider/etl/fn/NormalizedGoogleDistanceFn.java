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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.CrfOrConcept;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
public class NormalizedGoogleDistanceFn extends DoFn<KV<String, String>, KV<String, String>> {

	public enum CooccurLevel {
		DOCUMENT, SENTENCE, TITLE// , PARAGRAPH -- will require code to make sure all text is covered by a
									// paragraph in full text docs
	}

	private static final long serialVersionUID = 1L;

	public static Delimiter OUTPUT_FILE_DELIMITER = Delimiter.TAB;

	@SuppressWarnings("serial")
	public static TupleTag<String> SINGLETON_TO_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> PAIR_TO_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> DOCID_TO_CONCEPT_COUNT = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple computeCounts(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, CooccurLevel level,
			PCollectionView<Map<String, Set<String>>> ancestorMapView) {

		return statusEntityToText.apply("Identify concept annotations",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();

						Map<String, Set<String>> ancestorMap = context.sideInput(ancestorMapView);

						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();
						try {

							Set<String> singletonConceptIds = new HashSet<String>();
							Set<ConceptPair> pairedConceptIds = new HashSet<ConceptPair>();

							validateDocuments(docId, statusEntityToText.getValue(), requiredDocumentCriteria);

							countConcepts(docId, statusEntityToText.getValue(), ancestorMap, singletonConceptIds,
									pairedConceptIds, level);

							for (String conceptId : singletonConceptIds) {
								context.output(SINGLETON_TO_DOCID,
										String.format("%s%s%s", conceptId, OUTPUT_FILE_DELIMITER.delimiter(), docId));
							}

							for (ConceptPair pair : pairedConceptIds) {
								context.output(PAIR_TO_DOCID, String.format("%s%s%s", pair.toReproducibleKey(),
										OUTPUT_FILE_DELIMITER.delimiter(), docId));
							}

							context.output(DOCID_TO_CONCEPT_COUNT, String.format("%s%s%d", docId,
									OUTPUT_FILE_DELIMITER.delimiter(), singletonConceptIds.size()));
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
					private void validateDocuments(String documentId, Map<DocumentCriteria, String> docs,
							Set<DocumentCriteria> requiredDocumentCriteria) {
						if (!docs.keySet().equals(requiredDocumentCriteria)) {

							throw new IllegalArgumentException("Invalid document input (" + documentId + "). HAS: "
									+ docs.keySet().toString() + " BUT NEEDED: " + requiredDocumentCriteria.toString());

						}

					}

				}).withOutputTags(SINGLETON_TO_DOCID,
						TupleTagList.of(PAIR_TO_DOCID).and(DOCID_TO_CONCEPT_COUNT).and(ETL_FAILURE_TAG))
						.withSideInputs(ancestorMapView));
	}

	@VisibleForTesting
	protected static void countConcepts(String documentId, Map<DocumentCriteria, String> docs,
			Map<String, Set<String>> superClassMap, Set<String> singletonConceptIds, Set<ConceptPair> pairedConceptIds,
			CooccurLevel level) throws IOException {

		String documentText = PipelineMain.getDocumentText(docs);
		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docs);

		List<TextAnnotation> levelAnnots = getLevelAnnotations(documentId, level, documentText, docTypeToContentMap);

		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(DocumentType.CONCEPT_ALL);

		conceptAnnots = addSuperClassAnnotations(documentId, conceptAnnots, superClassMap);

		singletonConceptIds.addAll(getUniqueConceptIds(conceptAnnots));
		pairedConceptIds.addAll(getConceptPairs(conceptAnnots, levelAnnots));

	}

	@VisibleForTesting
	protected static Set<ConceptPair> getConceptPairs(Collection<TextAnnotation> conceptAnnots,
			Collection<TextAnnotation> levelAnnots) {
		Set<ConceptPair> pairs = new HashSet<ConceptPair>();

		Map<TextAnnotation, Set<TextAnnotation>> levelAnnotToConceptAnnotMap = matchConceptsToLevelAnnots(levelAnnots,
				conceptAnnots);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : levelAnnotToConceptAnnotMap.entrySet()) {
			for (TextAnnotation annot1 : entry.getValue()) {
				for (TextAnnotation annot2 : entry.getValue()) {
					String type1 = annot1.getClassMention().getMentionName();
					String type2 = annot2.getClassMention().getMentionName();
					// we require the types to be different and the annotation spans to also be
					// different
					if (!type1.equals(type2) && !annot1.getSpans().equals(annot2.getSpans())) {
						pairs.add(new ConceptPair(type1, type2));
					}
				}
			}
		}

		return pairs;
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
				if (concept.overlaps(level)) {
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

	private static Collection<? extends String> getUniqueConceptIds(Collection<TextAnnotation> conceptAnnots) {
		Set<String> conceptIds = new HashSet<String>();
		for (TextAnnotation annot : conceptAnnots) {
			conceptIds.add(annot.getClassMention().getMentionName());
		}
		return conceptIds;
	}

	/**
	 * Given a list of concept annotations and a mapping from concept identifiers to
	 * corresponding ancestor identifiers, return a list of the concept annotations
	 * augmented with annotations including all ancestor classes.
	 * 
	 * @param documentId
	 * @param conceptAnnots
	 * @param superClassMap
	 * @return
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> addSuperClassAnnotations(String documentId,
			Collection<TextAnnotation> conceptAnnots, Map<String, Set<String>> superClassMap) {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(documentId);
		Set<TextAnnotation> updatedAnnots = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : conceptAnnots) {
			updatedAnnots.add(annot);
			Set<String> superClassIds = superClassMap.get(annot.getClassMention().getMentionName());
			if (superClassIds != null) {
				for (String superClassId : superClassIds) {
					List<Span> spans = annot.getSpans();
					TextAnnotation newAnnot = factory.createAnnotation(spans.get(0).getSpanStart(),
							spans.get(0).getSpanEnd(), annot.getCoveredText(), superClassId);
					for (int i = 1; i < spans.size(); i++) {
						newAnnot.addSpan(spans.get(i));
					}
					updatedAnnots.add(newAnnot);
				}
			}
		}
		return updatedAnnots;
	}

//	/**
//	 * Given the input CRF and CONCEPT annotations, return a mapping from the
//	 * concept type, e.g. CHEBI, CL, etc. to a collection of CONCEPT annotations
//	 * that were filtered using the CRF annotations.
//	 * 
//	 * @param documentId
//	 * @param conceptTypeToContentMap
//	 * @param documentText
//	 * @return
//	 * @throws IOException
//	 */
//	@VisibleForTesting
//	protected static Map<String, Collection<TextAnnotation>> getConceptAnnotations(String documentId,
//			Map<String, Map<CrfOrConcept, String>> conceptTypeToContentMap, String documentText) throws IOException {
//
//		BioNLPDocumentReader reader = new BioNLPDocumentReader();
//		Map<String, Collection<TextAnnotation>> typeToAnnotMap = new HashMap<String, Collection<TextAnnotation>>();
//
//		for (Entry<String, Map<CrfOrConcept, String>> entry : conceptTypeToContentMap.entrySet()) {
//
//			String type = entry.getKey();
//			String crfBionlp = entry.getValue().get(CrfOrConcept.CRF);
//			String conceptBionlp = entry.getValue().get(CrfOrConcept.CONCEPT);
//
//			TextDocument crfDocument = reader.readDocument(documentId, "unknown",
//					new ByteArrayInputStream(crfBionlp.getBytes()), new ByteArrayInputStream(documentText.getBytes()),
//					CharacterEncoding.UTF_8);
//
//			TextDocument conceptDocument = reader.readDocument(documentId, "unknown",
//					new ByteArrayInputStream(conceptBionlp.getBytes()),
//					new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);
//
//			List<TextAnnotation> conceptAnnots = PipelineMain.filterViaCrf(conceptDocument.getAnnotations(),
//					crfDocument.getAnnotations());
//
//			typeToAnnotMap.put(type, conceptAnnots);
//
//		}
//		return typeToAnnotMap;
//	}

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

	@Getter
	@RequiredArgsConstructor
	@VisibleForTesting
	@SuppressWarnings("rawtypes")
	public static class ConceptPair extends DoFn {
		private static final long serialVersionUID = 1L;
		private final String conceptId1;
		private final String conceptId2;

		public String toReproducibleKey() {
			return (conceptId1.compareTo(conceptId2) < 1) ? conceptId1 + "|" + conceptId2
					: conceptId2 + "|" + conceptId1;
		}

		public static ConceptPair fromReproducibleKey(String s) {
			String[] cols = s.split("\\|");
			return new ConceptPair(cols[0], cols[1]);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ConceptPair other = (ConceptPair) obj;

			Set<String> set = CollectionsUtil.createSet(conceptId1, conceptId2);
			Set<String> otherSet = CollectionsUtil.createSet(other.getConceptId1(), other.getConceptId2());

			return set.equals(otherSet);
		}

		@Override
		public int hashCode() {
			Set<String> set = CollectionsUtil.createSet(conceptId1, conceptId2);
			return set.hashCode();
		}

		@Override
		public String toString() {
			return String.format("[%s, %s]", conceptId1, conceptId2);
		}

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
