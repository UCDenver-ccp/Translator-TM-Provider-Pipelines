package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
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
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.util.StopWordUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConceptPostProcessingFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	/**
	 * There are some matches that are created by the OGER normalization that are
	 * incorrect. We enumerate them here and remove them from the output.
	 */
	private static Map<String, Set<String>> ID_TO_TEXT_EXCLUSION_PAIRS = initIdToTextExclusionMap();

	// The below exclusion is now handled at the OGER level
//	public static final Set<String> NCBITAXON_IDS_TO_EXCLUDE = CollectionsUtil
//			.createSet("NCBITaxon:169495" /* matches "This" */);

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria,
			PCollectionView<Map<String, Set<String>>> extensionToOboMapView,
			PCollectionView<Map<String, String>> prPromotionMapView,
			PCollectionView<Map<String, Set<String>>> ncbiTaxonAncestorMapView,
//			PCollectionView<Map<String, Set<String>>> oboToAncestorsMapView, 
			FilterFlag filterFlag) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						Map<String, Set<String>> extensionToOboMap = context.sideInput(extensionToOboMapView);
						Map<String, String> prPromotionMap = context.sideInput(prPromotionMapView);
						Map<String, Set<String>> ncbitaxonPromotionMap = context.sideInput(ncbiTaxonAncestorMapView);
//						Map<String, Set<String>> oboToAncestorsMap = context.sideInput(oboToAncestorsMapView);

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (!docs.keySet().equals(requiredDocumentCriteria)) {
								PipelineMain.logFailure(ETL_FAILURE_TAG,
										"Unable to complete post-processing due to missing annotation documents for: "
												+ docId + " -- contains (" + statusEntityToText.getValue().size() + ") "
												+ statusEntityToText.getValue().keySet().toString(),
										outputDocCriteria, timestamp, out, docId, null);
							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(),
												statusEntityToText.getValue());


								Map<DocumentType, Collection<TextAnnotation>> docTypeToAnnotsMap = PipelineMain
										.filterConceptAnnotations(docTypeToContentMap, filterFlag);

//								throw new IllegalArgumentException(String.format("docTypeToAnnotsMap size for %s: %d -- %s",
//										docId, docTypeToAnnotsMap.size(), docTypeToAnnotsMap.keySet().toString()));

								
								Set<TextAnnotation> allAnnots = PipelineMain.spliceValues(docTypeToAnnotsMap.values());

								allAnnots = convertExtensionToObo(allAnnots, extensionToOboMap);
								allAnnots = promotePrAnnots(allAnnots, prPromotionMap);
								allAnnots = promoteNcbiTaxonAnnots(allAnnots, ncbitaxonPromotionMap);
								allAnnots = removeNcbiStopWords(allAnnots);
								allAnnots = removeIdToTextExclusionPairs(allAnnots);

								// the removals below should now be handled at the OGER level
//								allAnnots = excludeSelectNcbiTaxonAnnots(allAnnots);
//								allAnnots = removeIrrelevantHpConcepts(allAnnots, oboToAncestorsMap);
//								allAnnots = removeIrrelevantMondoConcepts(allAnnots, oboToAncestorsMap);
//								allAnnots = removeIrrelevantUberonConcepts(allAnnots, oboToAncestorsMap);
//								allAnnots = removeIrrelevantChebiConcepts(allAnnots, oboToAncestorsMap);

								String documentText = PipelineMain.getDocumentText(docs);
								TextDocument td = new TextDocument(statusEntity.getDocumentId(), "unknown",
										documentText);
								td.addAnnotations(allAnnots);
								BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
								String bionlp = null;
								try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
									writer.serialize(td, outputStream, CharacterEncoding.UTF_8);
									bionlp = outputStream.toString();
								}
								List<String> chunkedBionlp = PipelineMain.chunkContent(bionlp);
								out.get(ANNOTATIONS_TAG).output(KV.of(statusEntity, chunkedBionlp));
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during post processing",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withSideInputs(extensionToOboMapView, prPromotionMapView, ncbiTaxonAncestorMapView
//						,oboToAncestorsMapView
		).withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	@VisibleForTesting
	protected static Set<TextAnnotation> removeIdToTextExclusionPairs(Set<TextAnnotation> annots) {
		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>(annots);
		Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : annots) {
			String type = annot.getClassMention().getMentionName();
			if (ID_TO_TEXT_EXCLUSION_PAIRS.containsKey(type)) {
				Set<String> texts = ID_TO_TEXT_EXCLUSION_PAIRS.get(type);
				for (String text : texts) {
					if (annot.getCoveredText().equalsIgnoreCase(text)) {
						toRemove.add(annot);
						break;
					}
				}
			}
		}

		toKeep.removeAll(toRemove);

		return toKeep;
	}

	/**
	 * @return a map where the key is the concept curie and the set contains
	 *         instances of text that indicate an incorrect OGER identification of
	 *         the concept - typically this is due to a match resulting from
	 *         normalization of the original label
	 */
	private static Map<String, Set<String>> initIdToTextExclusionMap() {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		CollectionsUtil.addToOne2ManyUniqueMap("CL:0000540", "neuronal", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0043473", "pigmented", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0007349", "cellular", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0005694", "chromosomal", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0000062", "organisms", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0012131", "central", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0001451", "central", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0001427", "radial", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0012131", "centrally", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0001451", "centrally", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0001427", "radially", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0000094", "membrane organization", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0000160", "intestinal", map);
		CollectionsUtil.addToOne2ManyUniqueMap("HP:0030212", "collecting", map);

		return map;
	}

//	/**
//	 * only keep descendants of chemical substance or role
//	 * (http://purl.obolibrary.org/obo/CHEBI_59999,
//	 * http://purl.obolibrary.org/obo/CHEBI_50906)
//	 * 
//	 * @param annots
//	 * @param oboToAncestorsMap
//	 * @return
//	 */
//	private static Set<TextAnnotation> removeIrrelevantChebiConcepts(Set<TextAnnotation> annots,
//			Map<String, Set<String>> oboToAncestorsMap) {
//		String chemicalSubstance = "CHEBI:59999";
//		String role = "CHEBI:50906";
//		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
//		for (TextAnnotation annot : annots) {
//			String id = annot.getClassMention().getMentionName();
//			if (id.startsWith("CHEBI")) {
//				if (oboToAncestorsMap.containsKey(id) && (oboToAncestorsMap.get(id).contains(chemicalSubstance)
//						|| oboToAncestorsMap.get(id).contains(role))) {
//					toKeep.add(annot);
//				}
//			} else {
//				toKeep.add(annot);
//			}
//		}
//
//		return toKeep;
//	}
//
//	/**
//	 * only keep descendants of anatomical entity
//	 * (http://purl.obolibrary.org/obo/UBERON_0001062)
//	 * 
//	 * @param annots
//	 * @param oboToAncestorsMap
//	 * @return
//	 */
//	private static Set<TextAnnotation> removeIrrelevantUberonConcepts(Set<TextAnnotation> annots,
//			Map<String, Set<String>> oboToAncestorsMap) {
//		String anatomicalEntity = "UBERON:0001062";
//		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
//		for (TextAnnotation annot : annots) {
//			String id = annot.getClassMention().getMentionName();
//			if (id.startsWith("UBERON")) {
//				if (oboToAncestorsMap.containsKey(id) && oboToAncestorsMap.get(id).contains(anatomicalEntity)) {
//					toKeep.add(annot);
//				}
//			} else {
//				toKeep.add(annot);
//			}
//		}
//
//		return toKeep;
//	}
//
//	/**
//	 * only keep descendants of Disease or Disorder
//	 * (http://purl.obolibrary.org/obo/MONDO_0000001)
//	 * 
//	 * @param annots
//	 * @param oboToAncestorsMap
//	 * @return
//	 */
//	private static Set<TextAnnotation> removeIrrelevantMondoConcepts(Set<TextAnnotation> annots,
//			Map<String, Set<String>> oboToAncestorsMap) {
//		String diseaseOrDisorder = "MONDO:0000001";
//		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
//		for (TextAnnotation annot : annots) {
//			String id = annot.getClassMention().getMentionName();
//			if (id.startsWith("MONDO")) {
//				if (oboToAncestorsMap.containsKey(id) && oboToAncestorsMap.get(id).contains(diseaseOrDisorder)) {
//					toKeep.add(annot);
//				}
//			} else {
//				toKeep.add(annot);
//			}
//		}
//
//		return toKeep;
//	}
//
//	/**
//	 * only keep descendants of Phenotypic Abnormality
//	 * (http://purl.obolibrary.org/obo/HP_0000118)
//	 * 
//	 * @param annots
//	 * @param oboToAncestorsMap
//	 * @return
//	 */
//	private static Set<TextAnnotation> removeIrrelevantHpConcepts(Set<TextAnnotation> annots,
//			Map<String, Set<String>> oboToAncestorsMap) {
//
//		String phenotypicAbnormality = "HP:0000118";
//		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
//		for (TextAnnotation annot : annots) {
//			String id = annot.getClassMention().getMentionName();
//			if (id.startsWith("HP")) {
//				if (oboToAncestorsMap.containsKey(id) && oboToAncestorsMap.get(id).contains(phenotypicAbnormality)) {
//					toKeep.add(annot);
//				}
//			} else {
//				toKeep.add(annot);
//			}
//		}
//
//		return toKeep;
//	}

	@VisibleForTesting
	protected static Set<TextAnnotation> removeNcbiStopWords(Set<TextAnnotation> annots) {
		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
		Set<String> stopwords = new HashSet<String>(StopWordUtil.STOPWORDS);
		for (TextAnnotation annot : annots) {
			String coveredText = annot.getCoveredText();
			if (coveredText.length() > 2 && !stopwords.contains(coveredText.toLowerCase())) {
				// keep annotations that are not in the stopword list
				toKeep.add(annot);
			}
		}

		return toKeep;
	}

//	@VisibleForTesting
//	protected static Set<TextAnnotation> excludeSelectNcbiTaxonAnnots(Set<TextAnnotation> annots) {
//		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
//
//		for (TextAnnotation annot : annots) {
//			String type = annot.getClassMention().getMentionName();
//			if (!NCBITAXON_IDS_TO_EXCLUDE.contains(type)) {
//				// keep annotations that are not in the exclude list
//				toKeep.add(annot);
//			}
//		}
//
//		return toKeep;
//	}

	/**
	 * If there are taxon annotations with the same span, keep the more general
	 * class
	 * 
	 * @param allAnnots
	 * @param ncbitaxonAncestorMap
	 * @return
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> promoteNcbiTaxonAnnots(Set<TextAnnotation> allAnnots,
			Map<String, Set<String>> ncbitaxonAncestorMap) {

		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();

		Map<Span, Set<TextAnnotation>> spanToTaxonAnnotMap = new HashMap<Span, Set<TextAnnotation>>();
		for (TextAnnotation annot : allAnnots) {
			if (annot.getClassMention().getMentionName().startsWith("NCBITaxon:")) {
				CollectionsUtil.addToOne2ManyUniqueMap(annot.getAggregateSpan(), annot, spanToTaxonAnnotMap);
			} else {
				toKeep.add(annot);
			}
		}

		for (Entry<Span, Set<TextAnnotation>> entry : spanToTaxonAnnotMap.entrySet()) {
			Set<TextAnnotation> annots = entry.getValue();

			if (annots.size() > 1) {
				// keep more general class
				Map<String, TextAnnotation> typeToAnnotMap = new HashMap<String, TextAnnotation>();
				for (TextAnnotation annot : annots) {
					typeToAnnotMap.put(annot.getClassMention().getMentionName(), annot);
				}
				Set<String> typesToKeep = prefer(typeToAnnotMap.keySet(), ncbitaxonAncestorMap);
				for (String typeToKeep : typesToKeep) {
					toKeep.add(typeToAnnotMap.get(typeToKeep));
				}
			} else {
				// there is only one taxon annotation so keep it
				toKeep.addAll(annots);
			}

		}

		return toKeep;

	}

	@VisibleForTesting
	protected static Set<String> prefer(Set<String> ids, Map<String, Set<String>> ncbitaxonAncestorMap) {

		Set<String> toKeep = new HashSet<String>(ids);

		List<String> idList = new ArrayList<String>(ids);

		for (int i = 0; i < idList.size(); i++) {
			for (int j = 0; j < idList.size(); j++) {
				if (i != j) {
					String id1 = idList.get(i);
					String id2 = idList.get(j);

					Set<String> ancestors1 = ncbitaxonAncestorMap.get(id1);
					if (ancestors1.contains(id2)) {
						toKeep.remove(id1);
					} else {
						Set<String> ancestors2 = ncbitaxonAncestorMap.get(id2);
						if (ancestors2.contains(id1)) {
							toKeep.remove(id2);
						}
					}
				}
			}
			// if there is only one member in toKeep, then we can break out of the loops.
			if (toKeep.size() == 1) {
				break;
			}
		}

		return toKeep;
	}

	/**
	 * Given a set of annotations and a promotion map that maps from one class to
	 * the class it should be converted to, updated all of the annotations.
	 * 
	 * @param annots
	 * @param promotionMap
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> promotePrAnnots(Set<TextAnnotation> annots, Map<String, String> promotionMap) {

		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();
		for (TextAnnotation annot : annots) {
			String id = annot.getClassMention().getMentionName();
			if (promotionMap.containsKey(id)) {
				String promotedId = promotionMap.get(id);
				TextAnnotation ta = PipelineMain.clone(annot);
				ta.getClassMention().setMentionName(promotedId);
				toKeep.add(ta);
			} else {
				toKeep.add(PipelineMain.clone(annot));
			}
		}

		return toKeep;
	}

	/**
	 * Convert any CRAFT extension class identifiers into their corresponding OBO
	 * identifier(s)
	 * 
	 * @param annots
	 * @param extensionToOboMap
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> convertExtensionToObo(Set<TextAnnotation> annots,
			Map<String, Set<String>> extensionToOboMap) {

		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : annots) {
			String id = annot.getClassMention().getMentionName();
			if (extensionToOboMap.containsKey(id)) {
				for (String convertedId : extensionToOboMap.get(id)) {
					TextAnnotation ta = PipelineMain.clone(annot);
					ta.getClassMention().setMentionName(convertedId);
					toKeep.add(ta);
				}
			} else {
				toKeep.add(annot);
			}
		}

		return toKeep;

	}

}
