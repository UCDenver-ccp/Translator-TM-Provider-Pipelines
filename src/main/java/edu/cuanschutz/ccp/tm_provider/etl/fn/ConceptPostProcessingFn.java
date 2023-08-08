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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.text.similarity.LevenshteinDistance;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.ClassMention;
import edu.ucdenver.ccp.nlp.core.mention.ComplexSlotMention;
import edu.ucdenver.ccp.nlp.core.mention.SingleSlotFillerExpectedException;
import edu.ucdenver.ccp.nlp.core.util.StopWordUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConceptPostProcessingFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	private static final String HAS_SHORT_FORM_SLOT = "has_short_form";
	private static final String LONG_FORM_ABBREV_CLASS = "long_form";
	private static final String SHORT_FORM_ABBREV_CLASS = "short_form";

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
			PCollectionView<Map<String, Set<String>>> idToOgerDictEntriesMapView,
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
						Map<String, Set<String>> idToOgerDictEntriesMap = context.sideInput(idToOgerDictEntriesMapView);
						Map<String, Set<String>> ncbitaxonPromotionMap = context.sideInput(ncbiTaxonAncestorMapView);
//						Map<String, Set<String>> oboToAncestorsMap = context.sideInput(oboToAncestorsMapView);

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

								Map<DocumentType, Collection<TextAnnotation>> docTypeToAnnotsMap = PipelineMain
										.filterConceptAnnotations(docTypeToContentMap, filterFlag);

//								throw new IllegalArgumentException(String.format("docTypeToAnnotsMap size for %s: %d -- %s",
//										docId, docTypeToAnnotsMap.size(), docTypeToAnnotsMap.keySet().toString()));

								Collection<TextAnnotation> abbrevAnnots = docTypeToContentMap
										.get(DocumentType.ABBREVIATIONS);

								String documentText = PipelineMain.getDocumentText(docs);

								Set<TextAnnotation> allAnnots = PipelineMain.spliceValues(docTypeToAnnotsMap.values());

								Set<TextAnnotation> nonRedundantAnnots = new HashSet<TextAnnotation>();
								// remove all slots so that duplicate annotations will be condensed. There is a
								// slot for the theme id, e.g., T32, which will prevent duplicate annotations
								// from being condensed in the set -- the clone method creates a clone of the
								// annotation without any slots
								for (TextAnnotation annot : allAnnots) {
									nonRedundantAnnots.add(PipelineMain.clone(annot));
								}

								nonRedundantAnnots = convertExtensionToObo(nonRedundantAnnots, extensionToOboMap);
//								nonRedundantAnnots = promotePrAnnots(nonRedundantAnnots, prPromotionMap);
								nonRedundantAnnots = promoteNcbiTaxonAnnots(nonRedundantAnnots, ncbitaxonPromotionMap);
								nonRedundantAnnots = removeNcbiStopWords(nonRedundantAnnots);
								nonRedundantAnnots = removeIdToTextExclusionPairs(nonRedundantAnnots);
								nonRedundantAnnots = removeSpuriousMatches(nonRedundantAnnots, idToOgerDictEntriesMap);
								nonRedundantAnnots = removeMatchesLessThan(nonRedundantAnnots, 4);
								nonRedundantAnnots = removeAllAbbreviationShortFormAnnots(nonRedundantAnnots,
										abbrevAnnots);
								nonRedundantAnnots = propagateAbbreviationLongFormConceptsToShortFormMentions(
										nonRedundantAnnots, abbrevAnnots, docId, documentText);

								// check for duplicates
								List<TextAnnotation> annots = new ArrayList<TextAnnotation>(nonRedundantAnnots);
								for (int i = 0; i < annots.size(); i++) {
									for (int j = 0; j < annots.size(); j++) {
										if (i != j) {
											TextAnnotation annot1 = annots.get(i);
											TextAnnotation annot2 = annots.get(j);
											if (annot1.getClassMention().getMentionName()
													.equals(annot2.getClassMention().getMentionName())) {
												if (annot1.getAggregateSpan().equals(annot2.getAggregateSpan())) {
													throw new IllegalStateException("Found duplicates: \n"
															+ annot1.toString() + "\n" + annot2.toString());
												}
											}
										}

									}
								}

								// the removals below should now be handled at the OGER level
//								nonRedundantAnnots = excludeSelectNcbiTaxonAnnots(nonRedundantAnnots);
//								nonRedundantAnnots = removeIrrelevantHpConcepts(nonRedundantAnnots, oboToAncestorsMap);
//								nonRedundantAnnots = removeIrrelevantMondoConcepts(nonRedundantAnnots, oboToAncestorsMap);
//								nonRedundantAnnots = removeIrrelevantUberonConcepts(nonRedundantAnnots, oboToAncestorsMap);
//								nonRedundantAnnots = removeIrrelevantChebiConcepts(nonRedundantAnnots, oboToAncestorsMap);

								TextDocument td = new TextDocument(statusEntity.getDocumentId(), "unknown",
										documentText);
								td.addAnnotations(nonRedundantAnnots);
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

				}).withSideInputs(extensionToOboMapView, idToOgerDictEntriesMapView, ncbiTaxonAncestorMapView
//						,oboToAncestorsMapView
		).withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * For every abbreviation, look for mentions of the short form text in the
	 * document and remove any concept annotations that overlap with the short form
	 * mention
	 * 
	 * @param nonRedundantAnnots
	 * @param abbrevAnnots
	 * @return
	 */
	protected static Set<TextAnnotation> removeAllAbbreviationShortFormAnnots(Set<TextAnnotation> inputAnnots,
			Collection<TextAnnotation> abbrevAnnots) {

		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>(inputAnnots);
		Set<TextAnnotation> shortFormAnnots = getShortFormAnnots(abbrevAnnots);
		for (TextAnnotation annot : inputAnnots) {
			for (TextAnnotation shortFormAnnot : shortFormAnnots) {
				if (annot.overlaps(shortFormAnnot)) {
					toReturn.remove(annot);
					break;
				}
			}
		}

		return toReturn;
	}

	/**
	 * Returns the short form annotations for the input set of abbreviation
	 * annotations
	 * 
	 * @param abbrevAnnots
	 * @return
	 */
	public static Set<TextAnnotation> getShortFormAnnots(Collection<TextAnnotation> abbrevAnnots) {
		Set<TextAnnotation> shortFormAnnots = new HashSet<TextAnnotation>();
		for (TextAnnotation abbrevAnnot : abbrevAnnots) {
			String type = abbrevAnnot.getClassMention().getMentionName();
			if (type.equals(SHORT_FORM_ABBREV_CLASS)) {
				shortFormAnnots.add(abbrevAnnot);
			} else if (type.equals(LONG_FORM_ABBREV_CLASS)) {
				ComplexSlotMention csm = abbrevAnnot.getClassMention().getComplexSlotMentionByName(HAS_SHORT_FORM_SLOT);
				if (csm != null) {
					Collection<ClassMention> classMentions = csm.getClassMentions();
					for (ClassMention cm : classMentions) {
						shortFormAnnots.add(cm.getTextAnnotation());
					}
				}
			} else {
				// we don't expect to be here as there should only be short and long form
				// annotations
				throw new IllegalArgumentException("Observed abbreviation annotation of unexpected type: " + type
						+ " -- " + abbrevAnnot.getSingleLineRepresentation());
			}
		}

		return shortFormAnnots;
	}

	/**
	 * return the long form abbreviation annots from the input set. Long-form
	 * abbreviations are defined their type being "long_form" AND they must have a
	 * slot filler for the has_short_form slot.
	 * 
	 * @param abbrevAnnots
	 * @return
	 */
	public static Set<TextAnnotation> getLongFormAnnots(Collection<TextAnnotation> abbrevAnnots) {
		Set<TextAnnotation> longFormAnnots = new HashSet<TextAnnotation>();
		for (TextAnnotation abbrevAnnot : abbrevAnnots) {
			String type = abbrevAnnot.getClassMention().getMentionName();
			if (type.equals(LONG_FORM_ABBREV_CLASS)) {
				ComplexSlotMention csm = abbrevAnnot.getClassMention().getComplexSlotMentionByName(HAS_SHORT_FORM_SLOT);
				if (csm != null) {
					Collection<ClassMention> classMentions = csm.getClassMentions();
					if (classMentions.size() == 1) {
						longFormAnnots.add(abbrevAnnot);
					}
				}
			}
		}

		return longFormAnnots;
	}

	/**
	 * For every abbreviation, look to see if it overlaps substantially with an
	 * existing concept annotation. If it does, propagate that concept to all
	 * mentions of the short form text in the document.
	 * 
	 * @param nonRedundantAnnots
	 * @param abbrevAnnots
	 * @return
	 */
	protected static Set<TextAnnotation> propagateAbbreviationLongFormConceptsToShortFormMentions(
			Set<TextAnnotation> inputAnnots, Collection<TextAnnotation> abbrevAnnots, String docId,
			String documentText) {

		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(inputAnnots);

		Set<TextAnnotation> longFormAnnots = getLongFormAnnots(abbrevAnnots);

		Map<TextAnnotation, String> abbrevAnnotToConceptIdMap = matchConceptsToAbbreviations(inputAnnots,
				longFormAnnots);

		for (Entry<TextAnnotation, String> entry : abbrevAnnotToConceptIdMap.entrySet()) {
			TextAnnotation longAbbrevAnnot = entry.getKey();
			TextAnnotation shortAbbrevAnnot = getShortAbbrevAnnot(longAbbrevAnnot);
			String conceptId = entry.getValue();

			Set<TextAnnotation> propagatedShortAnnots = propagateShortAnnot(docId, documentText, shortAbbrevAnnot,
					conceptId);
			outputAnnots.addAll(propagatedShortAnnots);
		}

		return outputAnnots;

	}

	/**
	 * For each of the long form abbreviation annotations, look to see if there is
	 * an overlapping concept annotation. If there are multiple overlapping
	 * annotations, choose the longest. If there are multiple of the same size, then
	 * what??? -- maybe don't assign a concept ID since it it ambiguous.
	 * 
	 * @param inputAnnots
	 * @param abbrevAnnots
	 * @return
	 */
	private static Map<TextAnnotation, String> matchConceptsToAbbreviations(Set<TextAnnotation> conceptAnnots,
			Collection<TextAnnotation> longFormAbbrevAnnots) {

		Map<TextAnnotation, Set<TextAnnotation>> abbrevAnnotToConceptAnnotsMap = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		// TODO: this could be optimized potentially
		for (TextAnnotation longAbbrevAnnot : longFormAbbrevAnnots) {
			for (TextAnnotation conceptAnnot : conceptAnnots) {
				if (longAbbrevAnnot.overlaps(conceptAnnot)) {
					CollectionsUtil.addToOne2ManyUniqueMap(longAbbrevAnnot, conceptAnnot,
							abbrevAnnotToConceptAnnotsMap);
				}
			}
		}

		// select the appropriate overlapping concept if there are multiple and populate
		// the abbrevAnnotToConceptIdMap that is returned
		Map<TextAnnotation, String> abbrevAnnotToConceptIdMap = new HashMap<TextAnnotation, String>();

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : abbrevAnnotToConceptAnnotsMap.entrySet()) {
			TextAnnotation longAbbrevAnnot = entry.getKey();
			TextAnnotation overlappingConceptAnnot = selectConceptAnnot(entry.getValue());

			if (overlappingConceptAnnot != null) {
				String conceptId = overlappingConceptAnnot.getClassMention().getMentionName();
				abbrevAnnotToConceptIdMap.put(longAbbrevAnnot, conceptId);
			}
		}

		return abbrevAnnotToConceptIdMap;

	}

	/**
	 * If there is only one annotation, return it. If there are multiple, return the
	 * longest one. If there are multiple, but each are the same length, return null
	 * since we can't disambiguate.
	 * 
	 * @param value
	 * @return
	 */
	private static TextAnnotation selectConceptAnnot(Set<TextAnnotation> annots) {
		if (annots.size() == 1) {
			return annots.iterator().next();
		}
		Map<Integer, Set<TextAnnotation>> map = new HashMap<Integer, Set<TextAnnotation>>();
		for (TextAnnotation annot : annots) {
			CollectionsUtil.addToOne2ManyUniqueMap(annot.length(), annot, map);
		}

		Map<Integer, Set<TextAnnotation>> sortedMap = CollectionsUtil.sortMapByKeys(map, SortOrder.DESCENDING);

		// if the first entry only has a single text annotation then return it. If it
		// has multiple, return null;
		Set<TextAnnotation> longestAnnots = sortedMap.entrySet().iterator().next().getValue();
		if (longestAnnots.size() == 1) {
			return longestAnnots.iterator().next();
		}

		return null;

	}

	/**
	 * Look for the coveredtext of the specified short form abbreviation within the
	 * document text and create new annotations with the specified concept id for
	 * each occurrence of the text.
	 * 
	 * @param documentText
	 * @param shortAbbrevAnnot
	 * @param conceptId
	 * @return
	 */
	private static Set<TextAnnotation> propagateShortAnnot(String docId, String documentText,
			TextAnnotation shortAbbrevAnnot, String conceptId) {
		Set<TextAnnotation> propagagedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Pattern p = Pattern.compile(String.format("\\b(%s)\\b", shortAbbrevAnnot.getCoveredText()));
		Matcher m = p.matcher(documentText);
		while (m.find()) {
			String coveredText = m.group(1);
			int spanStart = m.start(1);
			int spanEnd = m.end(1);
			TextAnnotation annot = factory.createAnnotation(spanStart, spanEnd, coveredText, conceptId);
			propagagedAnnots.add(annot);
		}
		return propagagedAnnots;
	}

	/**
	 * Return the short abbrevation annot for the specified long abbreviation annot
	 * 
	 * @param longAbbrevAnnot
	 * @return
	 */
	public static TextAnnotation getShortAbbrevAnnot(TextAnnotation longAbbrevAnnot) {
		ComplexSlotMention csm = longAbbrevAnnot.getClassMention().getComplexSlotMentionByName(HAS_SHORT_FORM_SLOT);
		return csm.getSingleSlotValue().getTextAnnotation();
	}

	/**
	 * remove annotations whose covered text is less than the specified threshold
	 * 
	 * @param nonRedundantAnnots
	 * @param lengthThreshold
	 * @return
	 */
	protected static Set<TextAnnotation> removeMatchesLessThan(Set<TextAnnotation> annots, int lengthThreshold) {
		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : annots) {
			if (annot.getCoveredText().length() >= lengthThreshold) {
				toReturn.add(annot);
			}
		}

		return toReturn;
	}

	/**
	 * OGER sometimes matches substrings of a label and considers it a complete
	 * match for unknown reasons. This method attempts to filter out these spurious
	 * matches by comparing the matched text to the available dictionary entries for
	 * the concept. If there is not substantial agreement with one of the dictionary
	 * entries, then the match is excluded.
	 * 
	 * @param allAnnots
	 * @param idToOgerDictEntriesMap
	 * @return
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> removeSpuriousMatches(Set<TextAnnotation> allAnnots,
			Map<String, Set<String>> idToOgerDictEntriesMap) {
		LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();
		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : allAnnots) {
			String id = annot.getClassMention().getMentionName();
			String coveredText = annot.getCoveredText();

			// if the covered text is just digits and punctuation, then we will exclude it
			if (isDigitsAndPunctOnly(coveredText)) {
				continue;
			}

			if (idToOgerDictEntriesMap.containsKey(id)) {
				Set<String> dictEntries = idToOgerDictEntriesMap.get(id);
				for (String dictEntry : dictEntries) {
					Integer dist = ld.apply(coveredText.toLowerCase(), dictEntry.toLowerCase());
					float percentChange = (float) dist / (float) dictEntry.length();
					if (coveredText.contains("/") && percentChange != 0.0) {
						// slashes seem to cause lots of false positives, so we won't accept a slash in
						// a match unless it matches 100% with one of the dictionary entries
						continue;
					}
					if (percentChange < 0.3) {
						// 0.3 allows for some change due to normalizations

						// However, if the covered text is characters but the closest match is
						// characters with a number suffix, then we exclude, e.g. per matching Per1.
						boolean exclude = false;
						if (dictEntry.toLowerCase().startsWith(coveredText.toLowerCase())) {
							String suffix = StringUtil.removePrefix(dictEntry.toLowerCase(), coveredText.toLowerCase());
							if (suffix.matches("\\d+")) {
								exclude = true;
							}
						}

						if (!exclude) {
							toReturn.add(annot);
						}
					}
					System.out.println(String.format("%s -- %s == %d %f", coveredText, dictEntry, dist, percentChange));
				}
			}
		}

		return toReturn;
	}

	private static boolean isDigitsAndPunctOnly(String coveredText) {
		coveredText = coveredText.replaceAll("\\d", "");
		coveredText = coveredText.replace("\\p{Punct}", "");
		// actually if there is just one letter left, we will still count it as digits
		// and punct only
		return coveredText.trim().length() < 2;
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
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:0012131", "central", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:3010060", "central", map);
		CollectionsUtil.addToOne2ManyUniqueMap("UBERON:3010060", "centrally", map);
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
