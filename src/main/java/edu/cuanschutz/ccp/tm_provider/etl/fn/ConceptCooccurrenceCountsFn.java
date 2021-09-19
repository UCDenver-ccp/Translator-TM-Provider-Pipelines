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
import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ConceptCooccurrenceCountsPipeline.CountType;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.CrfOrConcept;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConceptCooccurrenceCountsFn extends DoFn<KV<String, String>, KV<String, String>> {

	public enum CooccurLevel {
		DOCUMENT(SINGLETON_TO_DOC_ID, PAIR_TO_DOC_ID, DOC_ID_TO_CONCEPT_COUNT),
		SENTENCE(SINGLETON_TO_SENTENCE_ID, PAIR_TO_SENTENCE_ID, SENTENCE_ID_TO_CONCEPT_COUNT),
		TITLE(SINGLETON_TO_TITLE_ID, PAIR_TO_TITLE_ID, TITLE_ID_TO_CONCEPT_COUNT),
		ABSTRACT(SINGLETON_TO_ABSTRACT_ID, PAIR_TO_ABSTRACT_ID, ABSTRACT_ID_TO_CONCEPT_COUNT);

		// , PARAGRAPH -- will require code to make sure all text is covered by a
		// paragraph in full text docs

		private final TupleTag<String> singletonTag;
		private final TupleTag<String> pairTag;
		private final TupleTag<String> idToCountTag;

		private CooccurLevel(TupleTag<String> singletonTag, TupleTag<String> pairTag, TupleTag<String> idToCountTag) {
			this.singletonTag = singletonTag;
			this.pairTag = pairTag;
			this.idToCountTag = idToCountTag;
		}

		public TupleTag<String> getSingletonTag() {
			return this.singletonTag;
		}

		public TupleTag<String> getPairTag() {
			return this.pairTag;
		}

		public TupleTag<String> getIdToCountTag() {
			return this.idToCountTag;
		}

	}

	public enum AddSuperClassAnnots {
		YES, NO
	}

	private static final long serialVersionUID = 1L;

	public static Delimiter OUTPUT_FILE_DELIMITER = Delimiter.TAB;

	@SuppressWarnings("serial")
	public static TupleTag<String> SINGLETON_TO_DOC_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> PAIR_TO_DOC_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> DOC_ID_TO_CONCEPT_COUNT = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<String> SINGLETON_TO_TITLE_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> PAIR_TO_TITLE_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> TITLE_ID_TO_CONCEPT_COUNT = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<String> SINGLETON_TO_SENTENCE_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> PAIR_TO_SENTENCE_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> SENTENCE_ID_TO_CONCEPT_COUNT = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<String> SINGLETON_TO_ABSTRACT_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> PAIR_TO_ABSTRACT_ID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> ABSTRACT_ID_TO_CONCEPT_COUNT = new TupleTag<String>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple computeCounts(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Set<CooccurLevel> levels,
			AddSuperClassAnnots addSuperClassAnnots, DocumentType docTypeToCount, CountType countType,
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

							// If the loaded document types don't match the required types, then throw an
							// exception which will get logged as a failure in datastore
							validatePresenceOfRequiredDocuments(docId, statusEntityToText.getValue(),
									requiredDocumentCriteria);

							for (CooccurLevel level : levels) {

								Map<String, Set<String>> textIdToSingletonConceptIds = new HashMap<String, Set<String>>();
								Map<String, Set<ConceptPair>> textIdToPairedConceptIds = new HashMap<String, Set<ConceptPair>>();

								countConcepts(docId, statusEntityToText.getValue(), ancestorMap,
										textIdToSingletonConceptIds, textIdToPairedConceptIds, level,
										addSuperClassAnnots, docTypeToCount);

								for (Entry<String, Set<String>> entry : textIdToSingletonConceptIds.entrySet()) {
									String textId = entry.getKey();
									Set<String> singletonConceptIds = entry.getValue();
									for (String conceptId : singletonConceptIds) {
										context.output(level.getSingletonTag(), String.format("%s%s%s", conceptId,
												OUTPUT_FILE_DELIMITER.delimiter(), textId));
									}
									// the following isn't necessary for the SIMPLE count type
									if (countType == CountType.FULL) {
										context.output(level.getIdToCountTag(), String.format("%s%s%d", textId,
												OUTPUT_FILE_DELIMITER.delimiter(), singletonConceptIds.size()));
									}
								}

								// the following isn't necessary for the SIMPLE count type
								if (countType == CountType.FULL) {
									for (Entry<String, Set<ConceptPair>> entry : textIdToPairedConceptIds.entrySet()) {
										String textId = entry.getKey();
										Set<ConceptPair> pairedConceptIds = entry.getValue();
										for (ConceptPair pair : pairedConceptIds) {
											context.output(level.getPairTag(),
													String.format("%s%s%s", pair.toReproducibleKey(),
															OUTPUT_FILE_DELIMITER.delimiter(), textId));
										}
									}

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

				}).withOutputTags(SINGLETON_TO_ABSTRACT_ID,
						TupleTagList.of(ETL_FAILURE_TAG).and(SINGLETON_TO_DOC_ID).and(SINGLETON_TO_SENTENCE_ID)
								.and(SINGLETON_TO_TITLE_ID).and(PAIR_TO_ABSTRACT_ID).and(PAIR_TO_DOC_ID)
								.and(PAIR_TO_SENTENCE_ID).and(PAIR_TO_TITLE_ID).and(ABSTRACT_ID_TO_CONCEPT_COUNT)
								.and(DOC_ID_TO_CONCEPT_COUNT).and(SENTENCE_ID_TO_CONCEPT_COUNT)
								.and(TITLE_ID_TO_CONCEPT_COUNT))
						.withSideInputs(ancestorMapView));
	}

	@VisibleForTesting
	protected static void countConcepts(String documentId, Map<DocumentCriteria, String> docs,
			Map<String, Set<String>> superClassMap, Map<String, Set<String>> textIdToSingletonConceptIds,
			Map<String, Set<ConceptPair>> textIdToPairedConceptIds, CooccurLevel level,
			AddSuperClassAnnots addSuperClassAnnots, DocumentType docTypeToCount) throws IOException {

		String documentText = PipelineMain.getDocumentText(docs);
		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docs);

		// levelAnnots is a list of annotations at the specified level, so if level ==
		// document then it's a single annotation representing the entire document.If
		// level == sentence, then it's the list of all sentences found in the document.
		List<TextAnnotation> levelAnnots = getLevelAnnotations(documentId, level, documentText, docTypeToContentMap);

		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(docTypeToCount);

		if (addSuperClassAnnots == AddSuperClassAnnots.YES) {
			conceptAnnots = addSuperClassAnnotations(documentId, conceptAnnots, superClassMap);
		}

		conceptAnnots = removeAnnotsWithMissingTypes(conceptAnnots);

		Map<TextAnnotation, Set<TextAnnotation>> levelAnnotToConceptAnnotMap = matchConceptsToLevelAnnots(levelAnnots,
				conceptAnnots);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : levelAnnotToConceptAnnotMap.entrySet()) {
			TextAnnotation levelAnnot = entry.getKey();
			Set<TextAnnotation> conceptAnnotsInLevel = entry.getValue();

			String textId = computeUniqueTextIdentifier(documentId, level, levelAnnot);
			Set<String> uniqueConceptsInLevelAnnot = getUniqueConceptIds(conceptAnnotsInLevel);
			Set<ConceptPair> conceptPairsInLevelAnnot = getConceptPairs(uniqueConceptsInLevelAnnot);
			if (!uniqueConceptsInLevelAnnot.isEmpty()) {
				textIdToSingletonConceptIds.put(textId, uniqueConceptsInLevelAnnot);
			}
			if (!conceptPairsInLevelAnnot.isEmpty()) {
				textIdToPairedConceptIds.put(textId, conceptPairsInLevelAnnot);
			}
		}

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
	private static Collection<TextAnnotation> removeAnnotsWithMissingTypes(Collection<TextAnnotation> conceptAnnots) {
		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
		for (TextAnnotation annot : conceptAnnots) {
			String type = annot.getClassMention().getMentionName();
			if (!type.trim().isEmpty()) {
				toKeep.add(annot);
			}
		}
		return toKeep;
	}

	@VisibleForTesting
	protected static Set<ConceptPair> getConceptPairs(Set<String> conceptIds) {
		Set<ConceptPair> pairs = new HashSet<ConceptPair>();

		for (String conceptId1 : conceptIds) {
			for (String conceptId2 : conceptIds) {
				if (!conceptId1.equals(conceptId2)) {
					pairs.add(new ConceptPair(conceptId1, conceptId2));
				}
			}
		}
		return pairs;
	}

//	@VisibleForTesting
//	protected static Set<ConceptPair> getConceptPairs(Collection<TextAnnotation> conceptAnnots) {
//		// Collection<TextAnnotation> levelAnnots) {
//		Set<ConceptPair> pairs = new HashSet<ConceptPair>();
//
////		Map<TextAnnotation, Set<TextAnnotation>> levelAnnotToConceptAnnotMap = matchConceptsToLevelAnnots(levelAnnots,
////				conceptAnnots);
//
////		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : levelAnnotToConceptAnnotMap.entrySet()) {
//		for (TextAnnotation annot1 : conceptAnnots) {
//			for (TextAnnotation annot2 : conceptAnnots) {
//				String type1 = annot1.getClassMention().getMentionName();
//				String type2 = annot2.getClassMention().getMentionName();
//				// we require the types to be different and the annotation spans to also be
//				// different
//				if (!type1.equals(type2) && !annot1.getSpans().equals(annot2.getSpans())) {
//					pairs.add(new ConceptPair(type1, type2));
//				}
//			}
//		}
////		}
//
//		return pairs;
//	}

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

	private static Set<String> getUniqueConceptIds(Collection<TextAnnotation> conceptAnnots) {
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
	 * Some ontologies, e.g. MONDO, link a large number of other "ontology concept",
	 * e.g. Mesh,ICD9, etc. This causes a combinatoric explosion when computing
	 * cooccurrence. To limit this issue, only superclasses with the same prefix as
	 * the seed class are added, e.g. for the class CHEBI_1234, only superclasses
	 * with the CHEBI prefix are added. This will also inherently avoid some of the
	 * upperlevel concepts that aren't likely to be that useful since they will
	 * cooccur with all/many concepts.
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
			String conceptId = annot.getClassMention().getMentionName();
			String conceptPrefix = null;
			if (conceptId.contains(":")) {
				conceptPrefix = conceptId.substring(0, conceptId.indexOf(":") + 1);
			}
			Set<String> superClassIds = superClassMap.get(conceptId);
			if (superClassIds != null) {
				for (String superClassId : superClassIds) {
					// if the concept id has a prefix, then only include superclasses that also have
					// the same prefix to limit the number of superclasses imported
					if (conceptPrefix == null || superClassId.startsWith(conceptPrefix)) {
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

		public String getPairId() {
			return DigestUtil.getBase64Sha1Digest(toReproducibleKey());
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
