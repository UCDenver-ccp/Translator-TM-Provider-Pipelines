package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import edu.cuanschutz.ccp.tm_provider.corpora.craft.ExcludeCraftNestedConcepts;
import edu.cuanschutz.ccp.tm_provider.corpora.craft.ExcludeCraftNestedConcepts.ExcludeExactOverlaps;
import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
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
			PCollectionView<Map<String, Set<String>>> ncbiTaxonAncestorMapView,
//			PCollectionView<Map<String, Set<String>>> oboToAncestorsMapView, 
			FilterFlag filterFlag, PipelineKey pipelineKey) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						Map<String, Set<String>> extensionToOboMap = null;
						if (extensionToOboMapView != null) {
							extensionToOboMap = context.sideInput(extensionToOboMapView);
						}

						Map<String, Set<String>> ncbitaxonPromotionMap = null;
						if (ncbiTaxonAncestorMapView != null) {
							// if reactivating this, need to re-add the map as a side input below (at end of
							// this method)
							ncbitaxonPromotionMap = context.sideInput(ncbiTaxonAncestorMapView);
						}

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (PipelineMain.requiredDocumentsArePresent(docs.keySet(), requiredDocumentCriteria,
									pipelineKey, ETL_FAILURE_TAG, docId, outputDocCriteria, timestamp, out)) {
//							if (!PipelineMain.fulfillsRequiredDocumentCriteria(docs.keySet(), requiredDocumentCriteria)) {
//								PipelineMain.logFailure(ETL_FAILURE_TAG,
//										"Unable to complete post-processing due to missing annotation documents for: "
//												+ docId + " -- contains (" + docs.size() + ") "
//												+ docs.keySet().toString(),
//										outputDocCriteria, timestamp, out, docId, null);
//							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(), docs);

								Map<DocumentType, Set<TextAnnotation>> docTypeToAnnotsMap = PipelineMain
										.filterConceptAnnotations(docTypeToContentMap, filterFlag);

								Collection<TextAnnotation> abbrevAnnots = docTypeToContentMap
										.get(DocumentType.ABBREVIATIONS);

								// this component requires the augmented document text - this text consists of
								// the original document text followed by sentences from the text that have been
								// added to the document in altered form so that they can also be processed by
								// the concept recognition machinery. Specifically, these are sentences that
								// contain abbreviation definitions where the short form part of the definition
								// has been replaced by whitespace. The replacement by whitespace allows for
								// concept recognition matches that may not otherwise occur.
								String augmentedDocumentText = PipelineMain.getAugmentedDocumentText(docs, docId);

								Set<TextAnnotation> allAnnots = PipelineMain.spliceValues(docTypeToAnnotsMap.values());

								Set<TextAnnotation> nonRedundantAnnots = new HashSet<TextAnnotation>();
								// remove all slots so that duplicate annotations will be condensed. There is a
								// slot for the theme id, e.g., T32, which will prevent duplicate annotations
								// from being condensed in the set -- the clone method creates a clone of the
								// annotation without any slots
								for (TextAnnotation annot : allAnnots) {
									nonRedundantAnnots.add(PipelineMain.clone(annot));
								}

								nonRedundantAnnots = postProcess(docId, extensionToOboMap, ncbitaxonPromotionMap,
										abbrevAnnots, augmentedDocumentText, nonRedundantAnnots);

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

								TextDocument td = new TextDocument(statusEntity.getDocumentId(), "unknown",
										augmentedDocumentText);
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

				})// .withSideInputs(ncbiTaxonAncestorMapView) //, extensionToOboMapView,)
				.withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * Modify the annotations via various post-processing techniques, most to remove
	 * likely false positives
	 * 
	 * @param docId
	 * @param extensionToOboMap
	 * @param idToOgerDictEntriesMap
	 * @param ncbitaxonPromotionMap
	 * @param abbrevAnnots
	 * @param augmentedDocumentText
	 * @param inputAnnots
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> postProcess(String docId, Map<String, Set<String>> extensionToOboMap,
			Map<String, Set<String>> ncbitaxonPromotionMap, Collection<TextAnnotation> abbrevAnnots,
			String augmentedDocumentText, Set<TextAnnotation> inputAnnots) throws FileNotFoundException, IOException {

		if (extensionToOboMap != null) {
			inputAnnots = convertExtensionToObo(inputAnnots, extensionToOboMap);
		}

		if (ncbitaxonPromotionMap != null) {
			inputAnnots = promoteNcbiTaxonAnnots(inputAnnots, ncbitaxonPromotionMap);
		}
		inputAnnots = removeNcbiStopWords(inputAnnots);
		inputAnnots = removeAnythingWithOddBracketCount(inputAnnots);
		inputAnnots = resolveHpMondoOverlaps(inputAnnots);
		inputAnnots = removeIdToTextExclusionPairs(inputAnnots);

		// moved to separate oger post-processing pipeline b/c its inclusion here was
		// causing the concept post processing pipeline to stall
//		inputAnnots = removeSpuriousMatches(inputAnnots, idToOgerDictEntriesMap);

		inputAnnots = removeMatchesLessThan(inputAnnots, 4);

		if (abbrevAnnots != null) {
			inputAnnots = removeAllAbbreviationShortFormAnnots(inputAnnots, abbrevAnnots);
			inputAnnots = propagateHybridAbbreviations(inputAnnots, abbrevAnnots, docId, augmentedDocumentText);
		}
		inputAnnots = filterAnnotsInAugmentedDocSection(inputAnnots, augmentedDocumentText);

		if (abbrevAnnots != null) {
			String originalDocumentText = getOriginalDocText(augmentedDocumentText);
			inputAnnots = propagateRegularAbbreviations(inputAnnots, abbrevAnnots, docId, originalDocumentText);
		}

		inputAnnots = removeNestedConceptAnnotations(inputAnnots);

		return inputAnnots;
	}

	/**
	 * It doesn't happen often, but there was at least one instance in the CRAFT
	 * training set: 14611657.concept_cs:T76 PR:000013158 13789 13793 PP{V
	 * 
	 * @param inputAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> removeAnythingWithOddBracketCount(Set<TextAnnotation> inputAnnots) {
		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(inputAnnots);
		for (TextAnnotation annot : inputAnnots) {
			String ct = annot.getCoveredText();
			// if there are any non-matching brackets, then we will exclude
			if (ct.contains("{") || ct.contains("}")) {
				if (countChar(ct, '{') - countChar(ct, '}') % 2 != 0) {
					outputAnnots.remove(annot);
				}
			}
			if (ct.contains("[") || ct.contains("]")) {
				if (countChar(ct, '[') - countChar(ct, ']') % 2 != 0) {
					outputAnnots.remove(annot);
				}
			}
			if (ct.contains("(") || ct.contains(")")) {
				if (countChar(ct, '(') - countChar(ct, ')') % 2 != 0) {
					outputAnnots.remove(annot);
				}
			}
		}
		return outputAnnots;
	}

	private static long countChar(String s, char c) {
		return s.chars().filter(ch -> ch == c).count();
	}

	/**
	 * This method removes annotations that are nested inside other annotations,
	 * e.g., it will remove the UBERON:blood annotation nested in the
	 * CL:red_blood_cell annotation.
	 * 
	 * @param inputAnnots
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static Set<TextAnnotation> removeNestedConceptAnnotations(Set<TextAnnotation> inputAnnots)
			throws FileNotFoundException, IOException {
		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(inputAnnots);

		Set<TextAnnotation> nestedAnnotations = ExcludeCraftNestedConcepts
				.identifyNestedAnnotations(new ArrayList<TextAnnotation>(inputAnnots), null, ExcludeExactOverlaps.NO);

//		craft-11597317.
//		T7	GO:0000725 428 450	recombinational repair
//		T8	SNOMEDCT:4365001 444 450	repair
//		
//		T12	GO:0000725 428 450	recombinational repair
//		T13	SNOMEDCT:4365001 444 450	repair

		outputAnnots.removeAll(nestedAnnotations);

		return outputAnnots;
	}

	/**
	 * Remove any concept annotation that appears in the augmented document text
	 * section - we are cleaning up after the hybrid abbreviation handling.
	 * 
	 * @param inputAnnots
	 * @param augmentedDocumentText
	 * @return
	 */
	protected static Set<TextAnnotation> filterAnnotsInAugmentedDocSection(Set<TextAnnotation> inputAnnots,
			String augmentedDocumentText) {
		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(inputAnnots);

		int augmentedSectionStart = augmentedDocumentText.indexOf(UtilityOgerDictFileFactory.DOCUMENT_END_MARKER);

		for (TextAnnotation annot : inputAnnots) {
			if (annot.getAnnotationSpanStart() >= augmentedSectionStart) {
				outputAnnots.remove(annot);
			}
		}

		return outputAnnots;
	}

	/**
	 * Return the original document text as extracted from the augmented document
	 * text
	 * 
	 * @param augmentedDocumentText
	 * @return
	 */
	private static String getOriginalDocText(String augmentedDocumentText) {
		return augmentedDocumentText.split(UtilityOgerDictFileFactory.DOCUMENT_END_MARKER)[0];
	}

	public enum Ont {
		HP, MONDO
	}

	/**
	 * If there is an HP annotation that has the identical span as a MONDO
	 * annotation, then we simply discard the HP annotation and keep the MONDO
	 * annotation. Doing so will allow the abbreviation code to work with the
	 * remaining MONDO annotation. Previously, because there are two overlapping
	 * annotations with the same span, the abbreviation code would not consider
	 * propagating a concept identifier b/c it can't disambiguate between two with
	 * identical spans.
	 * 
	 * @param inputAnnots
	 * @return
	 */
	private static Set<TextAnnotation> resolveHpMondoOverlaps(Set<TextAnnotation> inputAnnots) {
		Map<Span, Map<Ont, Set<TextAnnotation>>> map = new HashMap<Span, Map<Ont, Set<TextAnnotation>>>();
		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>(inputAnnots);
		for (TextAnnotation annot : inputAnnots) {
			String conceptId = annot.getClassMention().getMentionName();
			if (conceptId.startsWith("HP:") || conceptId.startsWith("MONDO:")) {
				Span span = annot.getAggregateSpan();
				if (!map.containsKey(span)) {
					Map<Ont, Set<TextAnnotation>> innerMap = new HashMap<Ont, Set<TextAnnotation>>();
					map.put(span, innerMap);
				}
				Ont ont = conceptId.startsWith("HP:") ? Ont.HP : Ont.MONDO;
				CollectionsUtil.addToOne2ManyUniqueMap(ont, annot, map.get(span));
			}
		}

		for (Entry<Span, Map<Ont, Set<TextAnnotation>>> entry : map.entrySet()) {
			Map<Ont, Set<TextAnnotation>> innerMap = entry.getValue();
			if (innerMap.size() > 1) {
				// grab the HP annot(s) and remove them from the set of annotations that gets
				// returned
				toReturn.removeAll(innerMap.get(Ont.HP));
			}
		}

		return toReturn;
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
		if (abbrevAnnots != null) {
			for (TextAnnotation abbrevAnnot : abbrevAnnots) {
				String type = abbrevAnnot.getClassMention().getMentionName();
				if (type.equals(LONG_FORM_ABBREV_CLASS)) {
					ComplexSlotMention csm = abbrevAnnot.getClassMention()
							.getComplexSlotMentionByName(HAS_SHORT_FORM_SLOT);
					if (csm != null) {
						Collection<ClassMention> classMentions = csm.getClassMentions();
						if (classMentions.size() == 1) {
							longFormAnnots.add(abbrevAnnot);
						}
					}
				}
			}
		}

		return longFormAnnots;
	}

	/**
	 * Hybrid abbreviations consist of an abbreviation that is used with a word or
	 * words later in the document. For example, "embryonic stem (ES) cells" is a
	 * hybrid abbreviation definition if later in the document we see use of "ES
	 * cells". These abbreviations are handled separately from regular abbreviations
	 * because sometimes the abbreviation part is a different type of entity, e.g.,
	 * maybe a disease, and we don't want to blindly propagate an incorrect entity,
	 * so we check to see if we can identify these hybrid abbreviations first.
	 * 
	 * @param inputAnnots
	 * @param abbrevAnnots
	 * @param docId
	 * @param augmentedDocumentText
	 * @return
	 */
	protected static Set<TextAnnotation> propagateHybridAbbreviations(Set<TextAnnotation> conceptAnnots,
			Collection<TextAnnotation> abbrevAnnots, String docId, String augmentedDocumentText) {

		String originalDocumentText = getOriginalDocText(augmentedDocumentText);

		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(conceptAnnots);

		// identify concept matches in the augmented sentences (sentences where the
		// short form abbreviation was removed) that weren't matched in the original
		// sentences

		// spans for these new concepts should now be relative to the original document
		// text
		Set<TextAnnotation> newConceptsInAugmentedDocText = findConceptsOnlyInAugmentedText(conceptAnnots,
				augmentedDocumentText, docId);

		// at this point, the only reason there will be new concepts is due to hiding of
		// the abbreviation short forms, so all new concepts should overlap with
		// abbreviation definitions, but perhaps we should check anyway.
		Map<TextAnnotation, TextAnnotation> newConceptToLongformAbbrevAnnotMap = mapConceptsToAbbrevAnnots(
				newConceptsInAugmentedDocText, abbrevAnnots);

		outputAnnots.addAll(newConceptToLongformAbbrevAnnotMap.keySet());

		// for each concept/abbrev annot pair, look for short form + the end of the
		// match ,e.g., ES cells, in the rest of
		// the document.
		Map<String, String> shortFormPlusToConceptIdMap = mapShortFormPlusTextToConceptId(
				newConceptToLongformAbbrevAnnotMap, originalDocumentText);

		// propagate those kinds of matches and somehow prevent the ES propagation.
		for (Entry<String, String> entry : shortFormPlusToConceptIdMap.entrySet()) {
			String abbrevText = entry.getKey();
			String conceptId = entry.getValue();
			Set<TextAnnotation> propagatedShortAnnots = propagateShortAnnot(docId, originalDocumentText, abbrevText,
					conceptId);

			outputAnnots.addAll(propagatedShortAnnots);
		}

		// we won't remove these -- it is possible that they will appear without the
		// hybrid extra text, so we will allow the regular abbreviation processing to
		// make use of them. Ultimately, any overlapping annotations will be resolved
		// when we remove nested entity annotation. So, for example, any spurious ES
		// annotations will be removed b/c they overlap with the long "ES cells"
		// annotations.

		// remove the abbreviations that were propagated so that they won't be processed
		// by other downstream abbreviation components
//		Collection<TextAnnotation> hybridAbbrevAnnots = newConceptToLongformAbbrevAnnotMap.values();
//		abbrevAnnots.removeAll(hybridAbbrevAnnots);

		return outputAnnots;
	}

	/**
	 * for each concept that is overlapping an abbreviation, look for any trailing
	 * text after the short form. Combine that trailing text with the short form
	 * abbreviation text, and look to see if that combination text appears later in
	 * the document text. If it does appear, add a map entry.
	 * 
	 * @param newConceptToLongformAbbrevAnnotMap
	 * @param originalDocumentText
	 * @return
	 */
	@VisibleForTesting
	protected static Map<String, String> mapShortFormPlusTextToConceptId(
			Map<TextAnnotation, TextAnnotation> conceptToAbbrevMap, String originalDocumentText) {
		Map<String, String> map = new HashMap<String, String>();

		for (Entry<TextAnnotation, TextAnnotation> entry : conceptToAbbrevMap.entrySet()) {
			TextAnnotation conceptAnnot = entry.getKey();
			TextAnnotation longAbbrevAnnot = entry.getValue();

			TextAnnotation shortAbbrevAnnot = getShortAbbrevAnnot(longAbbrevAnnot);

			int conceptEnd = conceptAnnot.getAnnotationSpanEnd();
			// +1 to include the likely parenthesis
			int shortAbbrevEnd = shortAbbrevAnnot.getAnnotationSpanEnd() + 1;

			if (conceptEnd > shortAbbrevEnd) {
				String hybridEnd = originalDocumentText.substring(shortAbbrevEnd, conceptEnd);
				String hybridAbbrevText = originalDocumentText.substring(shortAbbrevAnnot.getAnnotationSpanStart(),
						shortAbbrevAnnot.getAnnotationSpanEnd()) + hybridEnd;

				Pattern p = Pattern.compile(String.format("\\b(%s)\\b", hybridAbbrevText));
				Matcher m = p.matcher(originalDocumentText);
				int count = 0;
				while (m.find()) {
					count++;
				}

				// if we see any of the hybrid text in the document, then we add an entry from
				// the hybrid text to the concept id
				if (count > 0) {
					map.put(hybridAbbrevText, conceptAnnot.getClassMention().getMentionName());
				}

				// allow for plurals
				if (hybridAbbrevText.endsWith("s")) {
					hybridAbbrevText = StringUtil.removeSuffix(hybridAbbrevText, "s");
				} else {
					hybridAbbrevText = hybridAbbrevText + "s";
				}

				p = Pattern.compile(String.format("\\b(%s)\\b", hybridAbbrevText));
				m = p.matcher(originalDocumentText);
				count = 0;
				while (m.find()) {
					count++;
				}

				// if we see any of the hybrid text in the document, then we add an entry from
				// the hybrid text to the concept id
				if (count > 0) {
					map.put(hybridAbbrevText, conceptAnnot.getClassMention().getMentionName());
				}

			}
		}

		return map;
	}

	/**
	 * Create a mapping from the input concept annotations to abbreviation
	 * annotations that they overlap
	 * 
	 * @param newConceptsInAugmentedDocText
	 * @param abbrevAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static Map<TextAnnotation, TextAnnotation> mapConceptsToAbbrevAnnots(Set<TextAnnotation> conceptAnnots,
			Collection<TextAnnotation> abbrevAnnots) {

		Map<TextAnnotation, TextAnnotation> map = new HashMap<TextAnnotation, TextAnnotation>();

		for (TextAnnotation conceptAnnot : conceptAnnots) {
			for (TextAnnotation abbrevAnnot : getLongFormAnnots(abbrevAnnots)) {
				if (conceptAnnot.overlaps(abbrevAnnot)) {
					map.put(conceptAnnot, abbrevAnnot);
				}
			}
		}

		return map;
	}

	/**
	 * Looks at concepts in the augmented portion of the document text and compares
	 * them to the concepts in the original document text, returning concepts that
	 * are not present in the original document text. The concept annotations that
	 * are returned have been updated such that their spans are relevant to the
	 * original document text.
	 * 
	 * @param conceptAnnots
	 * @param augmentedDocumentText
	 * @param docId
	 * @return
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> findConceptsOnlyInAugmentedText(Set<TextAnnotation> conceptAnnots,
			String augmentedDocumentText, String docId) {

		Set<TextAnnotation> newConceptAnnots = new HashSet<TextAnnotation>();

		List<AugmentedSentence> augmentedSentences = getAugmentedSentences(augmentedDocumentText, docId);

		// concept annot spans should be relative to the original document text
		Map<AugmentedSentence, Set<TextAnnotation>> augSentToAugConceptAnnotsMap = mapSentToConceptAnnots(
				augmentedSentences, conceptAnnots, Overlap.AUG_SENTENCE);

		Map<AugmentedSentence, Set<TextAnnotation>> augSentToOrigConceptAnnotsMap = mapSentToConceptAnnots(
				augmentedSentences, conceptAnnots, Overlap.ORIG_SENTENCE);

		// the spans of the annotations in the two maps above have been aligned such
		// that they are all relative to the original document text. So, if there was a
		// concept that was identified in the augmented text, its annotation is
		// included, however its span has been converted such that it is relative to the
		// original sentence.

		// we loop over the augSentToAugConceptAnnotsMap because it might have
		// annotations in a sentence that the original does not
		for (Entry<AugmentedSentence, Set<TextAnnotation>> entry : augSentToAugConceptAnnotsMap.entrySet()) {
			AugmentedSentence augSent = entry.getKey();

			Set<TextAnnotation> augConceptAnnots = entry.getValue();
			Set<TextAnnotation> origConceptAnnots = augSentToOrigConceptAnnotsMap.get(augSent);

			// we should be left with any "new" concept annots that appear only in the
			// augmented text
			if (origConceptAnnots != null) {
				augConceptAnnots.removeAll(origConceptAnnots);
			}
//			origConceptAnnots.removeAll(augConceptAnnots);

			if (augConceptAnnots.size() > 0) {
				// if there is > 1 new concept, then we need to filter to get just one (I think)
				TextAnnotation newConceptAnnot = filterNewConceptAnnot(augConceptAnnots, augSent);
				if (newConceptAnnot != null) {
					newConceptAnnots.add(newConceptAnnot);
				}
			}
		}

		return newConceptAnnots;
	}

	/**
	 * It is possible that the augmented text is matched by more than one concept.
	 * This method is used to filter down to a single concept. We are going to
	 * require that the entire abbreviation be encompassed by the concept
	 * annotation, and we will select the longest concept annotation if there are
	 * multiple that meet the encompassing criteria.
	 * 
	 * @param augConceptAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static TextAnnotation filterNewConceptAnnot(Set<TextAnnotation> augConceptAnnots,
			AugmentedSentence augSent) {

//		System.out.println(String.format("augsent - orig span start: %d -- abbrev span: [%d, %d] ",
//				augSent.getOriginalSpanStart(), augSent.getAbbrevSpanStart(), augSent.getAbbrevSpanEnd()));

		Map<Integer, Set<TextAnnotation>> lengthToConceptAnnotMap = new HashMap<Integer, Set<TextAnnotation>>();

		Span abbrevSpan = new Span(augSent.getAbbrevSpanStart(), augSent.getAbbrevSpanEnd());
		for (TextAnnotation conceptAnnot : augConceptAnnots) {
//			int offset = augSent.getAnnot().getAnnotationSpanStart() - augSent.getOriginalSpanStart();
//			Span relativeConceptSpan = new Span(conceptAnnot.getAnnotationSpanStart() - offset,
//					conceptAnnot.getAnnotationSpanEnd() - offset);
//			System.out.println("relative span: " + relativeConceptSpan.toString());

//			if (relativeConceptSpan.containsSpan(abbrevSpan)) {
			if (conceptAnnot.getAggregateSpan().containsSpan(abbrevSpan)) {
				CollectionsUtil.addToOne2ManyUniqueMap(conceptAnnot.length(), conceptAnnot, lengthToConceptAnnotMap);
			}
		}

		Map<Integer, Set<TextAnnotation>> sortedMap = CollectionsUtil.sortMapByKeys(lengthToConceptAnnotMap,
				SortOrder.DESCENDING);

		for (Entry<Integer, Set<TextAnnotation>> entry : sortedMap.entrySet()) {
			Set<TextAnnotation> conceptAnnots = entry.getValue();

			if (conceptAnnots.size() == 1) {
				return conceptAnnots.iterator().next();
			} else {
				// we have a tie and no way to break the tie, so we won't use this abbreviation
				// to propagate new concept annotations.
				// TODO: potentially break tie with embeddings usage
				break;
			}
		}
		return null;
	}

	public enum Overlap {
		ORIG_SENTENCE, AUG_SENTENCE
	}

	/**
	 * Create a mapping from the augmented sentence object to the original concept
	 * annotations, i.e., those that were observed in the original (non-augmented)
	 * document text.
	 * 
	 * @param augmentedSentences
	 * @param conceptAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static Map<AugmentedSentence, Set<TextAnnotation>> mapSentToConceptAnnots(
			List<AugmentedSentence> augmentedSentences, Set<TextAnnotation> conceptAnnots, Overlap relative) {

//		for (TextAnnotation conceptAnnot : conceptAnnots) {
//			System.out.println(String.format("INPUT %s %s %s", conceptAnnot.getCoveredText(),
//					conceptAnnot.getSpans().toString(), conceptAnnot.getClassMention().getMentionName()));
//		}

//		System.out.println("======= mapping " + relative.name() + " ========");
		Map<AugmentedSentence, Set<TextAnnotation>> map = new HashMap<AugmentedSentence, Set<TextAnnotation>>();

		for (AugmentedSentence augSent : augmentedSentences) {
			for (TextAnnotation conceptAnnot : conceptAnnots) {
				Span sentSpan = (relative == Overlap.AUG_SENTENCE) ? augSent.getAnnot().getAggregateSpan()
						: new Span(augSent.getOriginalSpanStart(),
								augSent.getOriginalSpanStart() + augSent.getAnnot().getCoveredText().length());
				if (conceptAnnot.getAggregateSpan().overlaps(sentSpan)) {

					if (relative == Overlap.AUG_SENTENCE) {
						int offset = augSent.getAnnot().getAnnotationSpanStart() - augSent.getOriginalSpanStart();
						Span relativeSpan = new Span(conceptAnnot.getAnnotationSpanStart() - offset,
								conceptAnnot.getAnnotationSpanEnd() - offset);
						// if we don't clone, then we are changing the input set of annotations which
						// has downstream effects
						conceptAnnot = PipelineMain.clone(conceptAnnot);
						conceptAnnot.setSpans(Arrays.asList(relativeSpan));
					}

//					System.out.println(String.format("Adding to AugSent %s - %s %s %s", augSent.getId(),
//							conceptAnnot.getCoveredText(), conceptAnnot.getSpans().toString(),
//							conceptAnnot.getClassMention().getMentionName()));
					CollectionsUtil.addToOne2ManyUniqueMap(augSent, conceptAnnot, map);
				}
			}
		}

		return map;

	}

	/**
	 * Create a mapping from the augmented sentences to the concepts that were
	 * observed in them, however the concept annotations must have their spans
	 * updated so that they are relative to the original sentence.
	 * 
	 * @param augmentedSentences
	 * @param conceptAnnots
	 * @return
	 */
	@VisibleForTesting
	protected static Map<AugmentedSentence, Set<TextAnnotation>> mapAugSentToAugConceptAnnots(
			List<AugmentedSentence> augmentedSentences, Set<TextAnnotation> conceptAnnots) {
		Map<AugmentedSentence, Set<TextAnnotation>> map = new HashMap<AugmentedSentence, Set<TextAnnotation>>();

		for (AugmentedSentence augSent : augmentedSentences) {
			for (TextAnnotation conceptAnnot : conceptAnnots) {
				if (conceptAnnot.overlaps(augSent.getAnnot())) {
					// update the concept annotation span such that it is relative to the original
					// sentence
					int offset = augSent.getAnnot().getAnnotationSpanStart() - augSent.getOriginalSpanStart();
					Span updatedSpan = new Span(conceptAnnot.getAnnotationSpanStart() - offset,
							conceptAnnot.getAnnotationSpanEnd() - offset);
					conceptAnnot.setSpans(Arrays.asList(updatedSpan));
					CollectionsUtil.addToOne2ManyUniqueMap(augSent, conceptAnnot, map);
				}
			}
		}

		return map;
	}

	/**
	 * parse the augmented doc text section and return a list of
	 * {@link AugmentedSentence} objects
	 * 
	 * @param documentTextWithAugmentedSection
	 * @return
	 */
	@VisibleForTesting
	protected static List<AugmentedSentence> getAugmentedSentences(String documentTextWithAugmentedSection,
			String docId) {
		List<AugmentedSentence> asList = new ArrayList<AugmentedSentence>();

		String docText = documentTextWithAugmentedSection.split(UtilityOgerDictFileFactory.DOCUMENT_END_MARKER)[0];

		String augmentedText = documentTextWithAugmentedSection
				.split(UtilityOgerDictFileFactory.DOCUMENT_END_MARKER)[1];

		int offset = docText.length() + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER.length() + 1;
		String[] lines = augmentedText.split("\\n");
		// first line is a blank line, then there should be pairs of metadata + aug
		// sentence lines
		int id = 1;
		for (int i = 1; i < lines.length; i += 2) {
			String metadataLine = lines[i];
			offset += metadataLine.length() + 1;
			String sentenceLine = lines[i + 1];

			String[] cols = metadataLine.split("\\t");
			int origSentenceStart = Integer.parseInt(cols[1]);
			int abbrevStart = Integer.parseInt(cols[2]);
			int abbrevEnd = Integer.parseInt(cols[3]);

			TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);

			TextAnnotation sentAnnot = factory.createAnnotation(offset, sentenceLine.length() + offset, sentenceLine,
					"sentence");
			AugmentedSentence as = new AugmentedSentence(Integer.toString(id++), sentAnnot, origSentenceStart,
					abbrevStart, abbrevEnd);
			asList.add(as);
			offset += sentenceLine.length() + 1;
		}

		return asList;
	}

	@Data
	protected static class AugmentedSentence {
		private final String id;
		private final TextAnnotation annot;
		private final int originalSpanStart;
		private final int abbrevSpanStart;
		private final int abbrevSpanEnd;
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
	protected static Set<TextAnnotation> propagateRegularAbbreviations(Set<TextAnnotation> inputAnnots,
			Collection<TextAnnotation> abbrevAnnots, String docId, String documentText) {

		Set<TextAnnotation> outputAnnots = new HashSet<TextAnnotation>(inputAnnots);

		Set<TextAnnotation> longFormAnnots = getLongFormAnnots(abbrevAnnots);

		Map<TextAnnotation, String> abbrevAnnotToConceptIdMap = matchConceptsToAbbreviations(inputAnnots,
				longFormAnnots);

		for (Entry<TextAnnotation, String> entry : abbrevAnnotToConceptIdMap.entrySet()) {
			TextAnnotation longAbbrevAnnot = entry.getKey();
			TextAnnotation shortAbbrevAnnot = getShortAbbrevAnnot(longAbbrevAnnot);
			String conceptId = entry.getValue();

			Set<TextAnnotation> propagatedShortAnnots = propagateShortAnnot(docId, documentText,
					shortAbbrevAnnot.getCoveredText(), conceptId);
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
		LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : abbrevAnnotToConceptAnnotsMap.entrySet()) {
			TextAnnotation longAbbrevAnnot = entry.getKey();
			TextAnnotation overlappingConceptAnnot = selectConceptAnnot(entry.getValue());

			// require a significant level of overlap in order to assign the concept id to
			// the abbreviation
			// we'll start with 90% overlap

			// will will also look at what is not matched. If the only thing remaining not
			// matched is a number, especially if it's at the end of the abbreviation
			// covered text, then we will not assign the identifier. Not matching the number
			// can be a strong indicator that the concept identifier is not correct.

			if (overlappingConceptAnnot != null) {
				String longAbbrevText = longAbbrevAnnot.getCoveredText();
				String overlappingAnnotText = overlappingConceptAnnot.getCoveredText();
				Integer dist = ld.apply(longAbbrevText, overlappingAnnotText);
				float percentChange = (float) dist / (float) longAbbrevText.length();
				if (percentChange < 0.1) {
					// if the difference is just the number at the end of the label, then do not
					// match
					boolean skip = false;
					if (StringUtil.endsWithRegex(longAbbrevText, "\\d+")
							&& !StringUtil.endsWithRegex(overlappingAnnotText, "\\d+")) {
						skip = true;
					}
					if (!skip) {
						String conceptId = overlappingConceptAnnot.getClassMention().getMentionName();
						abbrevAnnotToConceptIdMap.put(longAbbrevAnnot, conceptId);
					}
				}
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
	private static Set<TextAnnotation> propagateShortAnnot(String docId, String documentText, String abbrevText,
			String conceptId) {
		Set<TextAnnotation> propagagedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Pattern p = Pattern.compile(String.format("\\b(%s)\\b", abbrevText));
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

//	/**
//	 * Moved to the OgerPostProcessing pipeline
//	 * OGER sometimes matches substrings of a label and considers it a complete
//	 * match for unknown reasons. This method attempts to filter out these spurious
//	 * matches by comparing the matched text to the available dictionary entries for
//	 * the concept. If there is not substantial agreement with one of the dictionary
//	 * entries, then the match is excluded.
//	 * 
//	 * @param allAnnots
//	 * @param idToOgerDictEntriesMap
//	 * @return
//	 */
//	@VisibleForTesting
//	protected static Set<TextAnnotation> removeSpuriousMatches(Set<TextAnnotation> allAnnots,
//			Map<String, String> idToOgerDictEntriesMap) {
//		LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();
//		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>();
//
//		for (TextAnnotation annot : allAnnots) {
//			String id = annot.getClassMention().getMentionName();
//			String coveredText = annot.getCoveredText();
//			// in the hybrid abbreviation handling, we sometimes get matches that contain
//			// consecutive whitespace, e.g., from where the short form portion of an
//			// abbreviation was removed in the covered text. Here we condense consecutive
//			// spaces into a single space so that the levenshstein distance calculation
//			// below isn't adversely affected.
//			coveredText = coveredText.replaceAll("\\s+", " ");
//
//			// if the covered text is just digits and punctuation, then we will exclude it
//			if (isDigitsAndPunctOnly(coveredText)) {
//				continue;
//			}
//
//			if (idToOgerDictEntriesMap.containsKey(id)) {
//				String dictEntries = idToOgerDictEntriesMap.get(id);
//				for (String dictEntry : dictEntries.split("\\|")) {
//					Integer dist = ld.apply(coveredText.toLowerCase(), dictEntry.toLowerCase());
//					float percentChange = (float) dist / (float) dictEntry.length();
//					if (coveredText.contains("/") && percentChange != 0.0) {
//						// slashes seem to cause lots of false positives, so we won't accept a slash in
//						// a match unless it matches 100% with one of the dictionary entries
//						continue;
//					}
//					if (percentChange < 0.3) {
//						// 0.3 allows for some change due to normalizations
//
//						// However, if the covered text is characters but the closest match is
//						// characters with a number suffix, then we exclude, e.g. per matching Per1.
//						boolean exclude = false;
//						if (dictEntry.toLowerCase().startsWith(coveredText.toLowerCase())) {
//							String suffix = StringUtil.removePrefix(dictEntry.toLowerCase(), coveredText.toLowerCase());
//							if (suffix.matches("\\d+")) {
//								exclude = true;
////								System.out.println("Excluding due to closest match has number suffix: " + annot.getAggregateSpan().toString() + " "
////										+ coveredText + " -- " + annot.getClassMention().getMentionName());
//							}
//						}
//
//						if (!exclude) {
//							toReturn.add(annot);
//						}
//					}
////					System.out.println(String.format("%s -- %s == %d %f", coveredText, dictEntry, dist, percentChange));
//				}
//			}
//		}
//
//		return toReturn;
//	}
//
//	private static boolean isDigitsAndPunctOnly(String coveredText) {
//		coveredText = coveredText.replaceAll("\\d", "");
//		coveredText = coveredText.replace("\\p{Punct}", "");
//		// actually if there is just one letter left, we will still count it as digits
//		// and punct only
//		return coveredText.trim().length() < 2;
//	}

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
			String id1 = idList.get(i);
			Set<String> ancestors1 = ncbitaxonAncestorMap.get(id1);
			for (int j = 0; j < idList.size(); j++) {
				if (i != j) {
					String id2 = idList.get(j);
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
		CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0005047", "sterile", map);

		CollectionsUtil.addToOne2ManyUniqueMap("GO:0051179", "local", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0008152", "metabolic", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0060073", "urine", map);
		CollectionsUtil.addToOne2ManyUniqueMap("GO:0007349", "cellular", map);

		return map;
	}

}
