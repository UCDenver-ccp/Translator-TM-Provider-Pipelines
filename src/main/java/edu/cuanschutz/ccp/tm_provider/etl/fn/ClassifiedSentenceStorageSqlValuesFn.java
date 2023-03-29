package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.AssertionTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.SPO;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ClassifiedSentenceStorageSqlValuesFn extends DoFn<KV<String, CoGbkResult>, AssertionTableValues> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<EvidenceScoreTableValues> EVIDENCE_SCORE_OUTPUT_TAG = new TupleTag<EvidenceScoreTableValues>() {
	};

//	@SuppressWarnings("serial")
//	public static TupleTag<SqlValues> SQLVALUES_OUTPUT_TAG = new TupleTag<SqlValues>() {
//	};

	@SuppressWarnings("serial")
	public static TupleTag<EntityTableValues> ENTITY_VALUES_OUTPUT_TAG = new TupleTag<EntityTableValues>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<AssertionTableValues> ASSERTION_VALUES_OUTPUT_TAG = new TupleTag<AssertionTableValues>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<EvidenceTableValues> EVIDENCE_VALUES_OUTPUT_TAG = new TupleTag<EvidenceTableValues>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, CoGbkResult>> results, TupleTag<String> bertOutputTag,
			TupleTag<String> metadataTag, BiolinkAssociation biolinkAssoc, double bertScoreInclusionMinimumThreshold) {
		return results.apply("compile sql values", ParDo.of(new DoFn<KV<String, CoGbkResult>, AssertionTableValues>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(@Element KV<String, CoGbkResult> element, MultiOutputReceiver out) {

				CoGbkResult result = element.getValue();
//				/*
//				 * the result should have one line from each file except for the header in the
//				 * bert output file which won't have a corresponding match in the metadata file.
//				 * Below we get one line for each tag, check to make sure there aren't other
//				 * lines just in case, and handle the case of the bert output file header.
//				 */
//				Iterator<String> bertOutputLineIter = result.getAll(bertOutputTag).iterator();
//				String bertOutputLine = getNextBertOutputLine(bertOutputLineIter);
//
//				Iterator<String> metadataLineIter = result.getAll(metadataTag).iterator();
//				String metadataLine = getNextMetadataOutputLine(metadataLineIter);

				/*
				 * the result should have one line from each file except for the header in the
				 * bert output file which won't have a corresponding match in the metadata file.
				 * Below we get one line for each tag, check to make sure there aren't other
				 * lines just in case, and handle the case of the bert output file header.
				 */
				String bertOutputLine = null;
				Iterator<String> bertOutputLineIter = result.getAll(bertOutputTag).iterator();
				if (bertOutputLineIter.hasNext()) {
					bertOutputLine = bertOutputLineIter.next();
					if (bertOutputLineIter.hasNext()) {
						// if there is another line, check it just to make sure it's a redundant copy.
						// If it's not identical, then error.
						String nextBertOutputLine = bertOutputLineIter.next();
						if (!nextBertOutputLine.equals(bertOutputLine)) {
							throw new IllegalArgumentException(
									"Did not expect another line to match from the BERT output file: " + bertOutputLine
											+ " --- != --- " + nextBertOutputLine);
						}
					}
				}

				String metadataLine = null;
				Iterator<String> metadataLineIter = result.getAll(metadataTag).iterator();
				if (metadataLineIter.hasNext()) {
					metadataLine = metadataLineIter.next();
					if (metadataLineIter.hasNext()) {
						// if there is another line, check it just to make sure it's a redundant copy.
						// If it's not identical, then error.
						//
						// Note: this does happen -- there was a case of a sentence segment in both
						// title and abstract -- document section probably needs to be used as part of
						// the hash for creating the sentence id.

						/*
						 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
						 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
						 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
						 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
						 * vector. title 2155 production of the synthetic drug anti-CD25/IL-10 using
						 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
						 * using minicircle vector. --- != ---
						 * 
						 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
						 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
						 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
						 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
						 * vector. abstract 2155 production of the synthetic drug anti-CD25/IL-10 using
						 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
						 * using minicircle vector.
						 * 
						 */
						String nextMetadataLine = metadataLineIter.next();
						if (!nextMetadataLine.equals(metadataLine)) {
							throw new IllegalArgumentException(
									"Did not expect another line to match from the metadata file: " + metadataLine
											+ " --- != --- " + nextMetadataLine);
						}
					}
				}

				List<AssertionTableValues> assertionTableValues = new ArrayList<AssertionTableValues>();
				List<EvidenceTableValues> evidenceTableValues = new ArrayList<EvidenceTableValues>();
				List<EntityTableValues> entityTableValues = new ArrayList<EntityTableValues>();
				List<EvidenceScoreTableValues> evidenceScoreTableValues = new ArrayList<EvidenceScoreTableValues>();

				processLines(biolinkAssoc, bertScoreInclusionMinimumThreshold, bertOutputLine, metadataLine,
						assertionTableValues, evidenceTableValues, entityTableValues, evidenceScoreTableValues);

				for (AssertionTableValues val : assertionTableValues) {
					out.get(ASSERTION_VALUES_OUTPUT_TAG).output(val);
				}

				for (EvidenceTableValues val : evidenceTableValues) {
					out.get(EVIDENCE_VALUES_OUTPUT_TAG).output(val);
				}

				for (EntityTableValues val : entityTableValues) {
					out.get(ENTITY_VALUES_OUTPUT_TAG).output(val);
				}

				for (EvidenceScoreTableValues val : evidenceScoreTableValues) {
					out.get(EVIDENCE_SCORE_OUTPUT_TAG).output(val);
				}

			}

			private String getNextMetadataOutputLine(Iterator<String> metadataLineIter) {
				String metadataLine = null;
				if (metadataLineIter.hasNext()) {
					metadataLine = metadataLineIter.next();
					if (metadataLineIter.hasNext()) {
						// if there is another line, check it just to make sure it's a redundant copy.
						// If it's not identical, then error.
						//
						// Note: this does happen -- there was a case of a sentence segment in both
						// title and abstract -- document section probably needs to be used as part of
						// the hash for creating the sentence id.

						/*
						 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
						 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
						 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
						 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
						 * vector. title 2155 production of the synthetic drug anti-CD25/IL-10 using
						 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
						 * using minicircle vector. --- != ---
						 * 
						 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
						 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
						 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
						 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
						 * vector. abstract 2155 production of the synthetic drug anti-CD25/IL-10 using
						 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
						 * using minicircle vector.
						 * 
						 */
						String nextMetadataLine = metadataLineIter.next();
						if (!nextMetadataLine.equals(metadataLine)) {
							throw new IllegalArgumentException(
									"Did not expect another line to match from the metadata file: " + metadataLine
											+ " --- != --- " + nextMetadataLine);
						}
					}
				}
				return metadataLine;
			}

			private String getNextBertOutputLine(Iterator<String> bertOutputLineIter) {
				String bertOutputLine = null;
				if (bertOutputLineIter.hasNext()) {
					bertOutputLine = bertOutputLineIter.next();
					if (bertOutputLineIter.hasNext()) {
						// if there is another line, check it just to make sure it's a redundant copy.
						// If it's not identical, then error.
						String nextBertOutputLine = bertOutputLineIter.next();
						if (!nextBertOutputLine.equals(bertOutputLine)) {
							throw new IllegalArgumentException(
									"Did not expect another line to match from the BERT output file: " + bertOutputLine
											+ " --- != --- " + nextBertOutputLine);
						}
					}
				}
				return bertOutputLine;
			}
		}).withOutputTags(ASSERTION_VALUES_OUTPUT_TAG, TupleTagList.of(EVIDENCE_SCORE_OUTPUT_TAG)
				.and(ENTITY_VALUES_OUTPUT_TAG).and(EVIDENCE_VALUES_OUTPUT_TAG)));
	}
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						KV<String, CoGbkResult> element = c.element();
//						CoGbkResult result = element.getValue();
//
//						/*
//						 * the result should have one line from each file except for the header in the
//						 * bert output file which won't have a corresponding match in the metadata file.
//						 * Below we get one line for each tag, check to make sure there aren't other
//						 * lines just in case, and handle the case of the bert output file header.
//						 */
//						String bertOutputLine = null;
//						Iterator<String> bertOutputLineIter = result.getAll(bertOutputTag).iterator();
//						if (bertOutputLineIter.hasNext()) {
//							bertOutputLine = bertOutputLineIter.next();
//							if (bertOutputLineIter.hasNext()) {
//								// if there is another line, check it just to make sure it's a redundant copy.
//								// If it's not identical, then error.
//								String nextBertOutputLine = bertOutputLineIter.next();
//								if (!nextBertOutputLine.equals(bertOutputLine)) {
//									throw new IllegalArgumentException(
//											"Did not expect another line to match from the BERT output file: "
//													+ bertOutputLine + " --- != --- " + nextBertOutputLine);
//								}
//							}
//						}
//
//						String metadataLine = null;
//						Iterator<String> metadataLineIter = result.getAll(metadataTag).iterator();
//						if (metadataLineIter.hasNext()) {
//							metadataLine = metadataLineIter.next();
//							if (metadataLineIter.hasNext()) {
//								// if there is another line, check it just to make sure it's a redundant copy.
//								// If it's not identical, then error.
//								//
//								// Note: this does happen -- there was a case of a sentence segment in both
//								// title and abstract -- document section probably needs to be used as part of
//								// the hash for creating the sentence id.
//
//								/*
//								 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
//								 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
//								 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
//								 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
//								 * vector. title 2155 production of the synthetic drug anti-CD25/IL-10 using
//								 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
//								 * using minicircle vector. --- != ---
//								 * 
//								 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
//								 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
//								 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
//								 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
//								 * vector. abstract 2155 production of the synthetic drug anti-CD25/IL-10 using
//								 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
//								 * using minicircle vector.
//								 * 
//								 */
//								String nextMetadataLine = metadataLineIter.next();
//								if (!nextMetadataLine.equals(metadataLine)) {
//									throw new IllegalArgumentException(
//											"Did not expect another line to match from the metadata file: "
//													+ metadataLine + " --- != --- " + nextMetadataLine);
//								}
//							}
//						}
//
//						/* both lines must have data in order to continue */
//						if (metadataLine != null && bertOutputLine != null) {
//
//							try {
//								int index = 0;
//								String[] bertOutputCols = bertOutputLine.split("\\t");
//								String sentenceId1 = bertOutputCols[index++];
//								@SuppressWarnings("unused")
//								String sentenceWithPlaceholders1 = bertOutputCols[index++];
//
//								// one of the scores for the predicates that is not
//								// BiolinkPredicate.NO_RELATION_PRESENT must be greater than the BERT minimum
//								// inclusion score threshold in order for this sentence to be considered
//								// evidence of an assertion. This is checked while populating the
//								// predicateCurietoScoreMap.
//								boolean hasScoreThatMeetsMinimumInclusionThreshold = false;
//								Map<String, Double> predicateCurieToScore = new HashMap<String, Double>();
//								for (String predicateCurie : getPredicateCuries(biolinkAssoc)) {
//									double score = Double.parseDouble(bertOutputCols[index++]);
//									predicateCurieToScore.put(predicateCurie, score);
//									if (!predicateCurie.equals("false") && score > bertScoreInclusionMinimumThreshold) {
//										hasScoreThatMeetsMinimumInclusionThreshold = true;
//									}
//
//								}
//
//								if (hasScoreThatMeetsMinimumInclusionThreshold) {
//									ExtractedSentence es = null;
//
//									try {
//										es = ExtractedSentence.fromTsv(metadataLine, true);
//									} catch (RuntimeException e) {
//										// some sentences in the PMCOA set contain tabs -- these need to be removed
//										// upstream. For now we catch the error so that processing can continue
//										es = null;
//									}
//									if (es != null) {
//
//										// if the IdSuffixToProcess is set then only process the sentence if it ends
//										// with that suffix
//										if (idSuffixToProcess.equals("null") || idSuffixToProcess.isEmpty()
//												|| idSuffixToProcess == null || (!idSuffixToProcess.isEmpty()
//														&& es.getDocumentId().endsWith(idSuffixToProcess))) {
//
//											// ensure the sentence identifer in the metadata line equals that from the
//											// bert
//											// output line. This check is probably unnecessary since the lines were
//											// keyed
//											// together based on the sentence ID initially, but we'll check just in
//											// case.
//											if (es.getSentenceIdentifier().equals(sentenceId1)) {
//												String subjectCoveredText;
//												String subjectCurie;
//												String subjectSpanStr;
//												String objectCoveredText;
//												String objectCurie;
//												String objectSpanStr;
//												if (es.getEntityPlaceholder1()
//														.equals(biolinkAssoc.getSubjectClass().getPlaceholder())) {
//													subjectCurie = es.getEntityId1();
//													subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
//													subjectCoveredText = es.getEntityCoveredText1();
//													objectCurie = es.getEntityId2();
//													objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
//													objectCoveredText = es.getEntityCoveredText2();
//												} else {
//													subjectCurie = es.getEntityId2();
//													subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
//													subjectCoveredText = es.getEntityCoveredText2();
//													objectCurie = es.getEntityId1();
//													objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
//													objectCoveredText = es.getEntityCoveredText1();
//												}
//
//												String documentId = es.getDocumentId();
//												String sentence = es.getSentenceText();
//
//												int documentYearPublished = es.getDocumentYearPublished();
//												if (documentYearPublished > 2155) {
//													// 2155 is the max year value in MySQL
//													documentYearPublished = 2155;
//												}
//
//												/*
//												 * for efficiency purposes, if there were multiple ontologies IDs
//												 * identified for the same span of text, those identifiers were spliced
//												 * together into a pipe-delimited list so that sentence only needed to
//												 * be classified once. Here we unsplice any spliced identifiers and
//												 * create separate sqlvalues for each.
//												 */
//												for (String sub : subjectCurie.split("\\|")) {
//													for (String obj : objectCurie.split("\\|")) {
//
//														String assertionId = DigestUtils
//																.sha256Hex(sub + obj + biolinkAssoc.getAssociationId());
//														String evidenceId = DigestUtils.sha256Hex(documentId + sentence
//																+ sub + subjectSpanStr + obj + objectSpanStr
//																+ biolinkAssoc.getAssociationId());
//														String subjectEntityId = DigestUtils
//																.sha256Hex(documentId + sentence + sub + subjectSpanStr
//																		+ biolinkAssoc.getAssociationId());
//														String objectEntityId = DigestUtils
//																.sha256Hex(documentId + sentence + obj + objectSpanStr
//																		+ biolinkAssoc.getAssociationId());
//
//														// make sure things will fit in the database columns
//
//														// sentences of this great length are most likely sentence
//														// segmentation
//														// errors.
//														// Here we truncate anything longer than 1900 characters so that
//														// it
//														// fits
//														// in
//														// the
//														// database column.
//														if (sentence.length() > 1900) {
//															sentence = sentence.substring(0, 1900);
//														}
//
//														if (sub.length() > 95) {
//															sub = sub.substring(0, 95);
//														}
//
//														if (obj.length() > 95) {
//															obj = obj.substring(0, 95);
//														}
//
//														String associationId = biolinkAssoc.getAssociationId();
//														if (associationId.length() > 95) {
//															associationId = associationId.substring(0, 95);
//														}
//
//														String documentZone = es.getDocumentZone();
//														if (documentZone.length() > 45) {
//															documentZone = documentZone.substring(0, 45);
//														}
//
//														String pubTypes = CollectionsUtil.createDelimitedString(
//																es.getDocumentPublicationTypes(), "|");
//														if (pubTypes.length() > 500) {
//															pubTypes = pubTypes.substring(0, 450);
//														}
//
//														if (subjectCoveredText.length() > 100) {
//															subjectCoveredText = subjectCoveredText.substring(0, 100);
//														}
//
//														if (objectCoveredText.length() > 100) {
//															objectCoveredText = objectCoveredText.substring(0, 100);
//														}
//
//														SqlValues sqlValues = new SqlValues(assertionId, sub, obj,
//																associationId, evidenceId, documentId, sentence,
//																subjectEntityId, objectEntityId, documentZone, pubTypes,
//																documentYearPublished, subjectSpanStr, objectSpanStr,
//																subjectCoveredText, objectCoveredText);
//
//														for (Entry<String, Double> entry : predicateCurieToScore
//																.entrySet()) {
//															String predicateCurie = entry.getKey();
//															if (predicateCurie.length() > 100) {
//																predicateCurie = predicateCurie.substring(0, 100);
//															}
//
//															sqlValues.addScore(predicateCurie, entry.getValue());
//														}
//
//														c.output(sqlValues);
//													}
//												}
//											} else {
//												throw new IllegalStateException(
//														"Mismatch between the BERT output file and sentence metadata files detected. "
//																+ "Sentence identifiers do not match!!! "
//																+ es.getSentenceIdentifier() + " != " + sentenceId1);
//											}
//										}
//									}
//								}
//							} catch (ArrayIndexOutOfBoundsException e) {
//								throw new IllegalArgumentException("ARRAY INDEX OOB on line: " + bertOutputLine, e);
//							}
//						}
//
//					}
//
//				}));
//	}

	private static boolean hasScoreThatMeetsMinimumInclusionThreshold(double bertScoreInclusionMinimumThreshold,
			Map<String, Double> predicateCurieToScore) {
		boolean hasScoreThatMeetsMinimumInclusionThreshold = false;
		for (Entry<String, Double> entry : predicateCurieToScore.entrySet()) {
			if (!entry.getKey().equals("false") && entry.getValue() > bertScoreInclusionMinimumThreshold) {
				hasScoreThatMeetsMinimumInclusionThreshold = true;
			}
		}
		return hasScoreThatMeetsMinimumInclusionThreshold;
	}

	private static Map<String, Double> getPredicateCurieToScoreMap(BiolinkAssociation biolinkAssoc,
			String[] bertOutputCols, int index) {
		// one of the scores for the predicates that is not
		// BiolinkPredicate.NO_RELATION_PRESENT must be greater than the BERT minimum
		// inclusion score threshold in order for this sentence to be considered
		// evidence of an assertion. This is checked while populating the
		// predicateCurietoScoreMap.

		int col = index;
		Map<String, Double> predicateCurieToScore = new HashMap<String, Double>();
		for (String predicateCurie : getPredicateCuries(biolinkAssoc)) {
			double score = Double.parseDouble(bertOutputCols[col++]);
			predicateCurieToScore.put(predicateCurie, score);
		}
		return predicateCurieToScore;
	}

	protected static void processLines(BiolinkAssociation biolinkAssoc, double bertScoreInclusionMinimumThreshold,
			String bertOutputLine, String metadataLine, List<AssertionTableValues> assertionTableValues,
			List<EvidenceTableValues> evidenceTableValues, List<EntityTableValues> entityTableValues,
			List<EvidenceScoreTableValues> evidenceScoreTableValues) {
		/* both lines must have data in order to continue */
		if (metadataLine != null && bertOutputLine != null) {

			try {
				int index = 0;
				String[] bertOutputCols = bertOutputLine.split("\\t");
				String sentenceId1 = bertOutputCols[index++];
				@SuppressWarnings("unused")
				String sentenceWithPlaceholders1 = bertOutputCols[index++];

				Map<String, Double> predicateCurieToScore = getPredicateCurieToScoreMap(biolinkAssoc, bertOutputCols,
						index);

				if (hasScoreThatMeetsMinimumInclusionThreshold(bertScoreInclusionMinimumThreshold,
						predicateCurieToScore)) {

					ExtractedSentence es = null;

					try {
						es = ExtractedSentence.fromTsv(metadataLine, true);
					} catch (RuntimeException e) {
						// some sentences in the PMCOA set contain tabs -- these need to be removed
						// upstream. For now we catch the error so that processing can continue
						e.printStackTrace();
						es = null;
					}
					if (es != null) {

//						// if the IdSuffixToProcess is set then only process the sentence if it ends
//						// with that suffix
//						if (idSuffixToProcess.equals("null") || idSuffixToProcess.isEmpty()
//								|| idSuffixToProcess == null || (!idSuffixToProcess.isEmpty()
//										&& es.getDocumentId().endsWith(idSuffixToProcess))) {

						// ensure the sentence identifer in the metadata line equals that from the
						// bert
						// output line. This check is probably unnecessary since the lines were
						// keyed
						// together based on the sentence ID initially, but we'll check just in
						// case.

						// we used to check for sentence identifier matches using
						// es.getSentenceIdentifier() however, I removed some ^M's from the metadata
						// files manually, so the es.getSentenceIdentifier() now computes a different
						// identifier because the sentence text is different. Instead we will parse the
						// sentence identifier from the
						// metadataLine directly.
//						if (es.getSentenceIdentifier().equals(sentenceId1)) {

						String metadataLineIdentifier = metadataLine.split("\\t")[0];
						if (metadataLineIdentifier.equals(sentenceId1)) {
							String subjectCoveredText;
							String subjectCurie;
							String subjectSpanStr;
							String objectCoveredText;
							String objectCurie;
							String objectSpanStr;
							if (es.getEntityPlaceholder1().equals(biolinkAssoc.getSubjectClass().getPlaceholder())) {
								subjectCurie = es.getEntityId1();
								subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
								subjectCoveredText = es.getEntityCoveredText1();
								objectCurie = es.getEntityId2();
								objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
								objectCoveredText = es.getEntityCoveredText2();
							} else {
								subjectCurie = es.getEntityId2();
								subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
								subjectCoveredText = es.getEntityCoveredText2();
								objectCurie = es.getEntityId1();
								objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
								objectCoveredText = es.getEntityCoveredText1();
							}

							String documentId = es.getDocumentId();
							String sentence = es.getSentenceText();

							int documentYearPublished = es.getDocumentYearPublished();
							if (documentYearPublished > 2155) {
								// 2155 is the max year value in MySQL
								documentYearPublished = 2155;
							}

							/*
							 * for efficiency purposes, if there were multiple ontologies IDs identified for
							 * the same span of text, those identifiers were spliced together into a
							 * pipe-delimited list so that sentence only needed to be classified once. Here
							 * we unsplice any spliced identifiers and create separate sqlvalues for each.
							 */
							for (String sub : subjectCurie.split("\\|")) {
								for (String obj : objectCurie.split("\\|")) {

									String assertionId = getAssertionId(biolinkAssoc, sub, obj);
									String evidenceId = getEvidenceId(biolinkAssoc, subjectSpanStr, objectSpanStr,
											documentId, sentence, sub, obj);
									String subjectEntityId = getEntityId(biolinkAssoc, subjectSpanStr, documentId,
											sentence, sub);
									String objectEntityId = getEntityId(biolinkAssoc, objectSpanStr, documentId,
											sentence, obj);

									// make sure things will fit in the database columns

									// sentences of this great length are most likely sentence
									// segmentation
									// errors.
									// Here we truncate anything longer than 1900 characters so that
									// it
									// fits
									// in
									// the
									// database column.
									if (sentence.length() > 1900) {
										sentence = sentence.substring(0, 1900);
									}

									if (sub.length() > 95) {
										sub = sub.substring(0, 95);
									}

									if (obj.length() > 95) {
										obj = obj.substring(0, 95);
									}

									String associationId = biolinkAssoc.getAssociationId();
									if (associationId.length() > 95) {
										associationId = associationId.substring(0, 95);
									}

									String documentZone = es.getDocumentZone();
									if (documentZone.length() > 45) {
										documentZone = documentZone.substring(0, 45);
									}

									String pubTypes = CollectionsUtil
											.createDelimitedString(es.getDocumentPublicationTypes(), "|");
									if (pubTypes.length() > 500) {
										pubTypes = pubTypes.substring(0, 450);
									}

									if (subjectCoveredText.length() > 100) {
										subjectCoveredText = subjectCoveredText.substring(0, 100);
									}

									if (objectCoveredText.length() > 100) {
										objectCoveredText = objectCoveredText.substring(0, 100);
									}

//									SqlValues sqlValues = new SqlValues(assertionId, sub, obj, associationId,
//											evidenceId, documentId, sentence, subjectEntityId, objectEntityId,
//											documentZone, pubTypes, documentYearPublished, subjectSpanStr,
//											objectSpanStr, subjectCoveredText, objectCoveredText);

//									if (subjectCurie.length() > 25) {
//										throw new IllegalArgumentException("LONG SUBJ CURIE: " + subjectCurie);
//									}
//									
//									if (objectCurie.length() > 25) {
//										throw new IllegalArgumentException("LONG OBJ CURIE: " + objectCurie);
//									}

									AssertionTableValues assertionValues = new AssertionTableValues(assertionId, sub,
											obj, associationId);
									assertionTableValues.add(assertionValues);

									EvidenceTableValues evidenceValues = new EvidenceTableValues(evidenceId,
											assertionId, documentId, sentence, subjectEntityId, objectEntityId,
											documentZone, pubTypes, documentYearPublished);
									evidenceTableValues.add(evidenceValues);

									EntityTableValues subjEntityValues = new EntityTableValues(subjectEntityId,
											subjectSpanStr, subjectCoveredText);
									EntityTableValues objEntityValues = new EntityTableValues(objectEntityId,
											objectSpanStr, objectCoveredText);

									entityTableValues.add(subjEntityValues);
									entityTableValues.add(objEntityValues);

									// we are adding the evidence_score stuff to the sqlValues -- this is no
									// longer necessary since we are now exporting the evidence_score data
									// separately, but we'll leave it as is.
									for (Entry<String, Double> entry : predicateCurieToScore.entrySet()) {
										String predicateCurie = entry.getKey();
										if (predicateCurie.length() > 100) {
											predicateCurie = predicateCurie.substring(0, 100);
										}

//										sqlValues.addScore(predicateCurie, entry.getValue());

										EvidenceScoreTableValues evtValues = new EvidenceScoreTableValues(evidenceId,
												predicateCurie, entry.getValue());

										System.out.println("Score: " + predicateCurie + " " + entry.getValue());

										evidenceScoreTableValues.add(evtValues);

									}
//									out.get(SQLVALUES_OUTPUT_TAG).output(sqlValues);
								}
							}
						} else {
							throw new IllegalStateException(
									"Mismatch between the BERT output file and sentence metadata files detected. "
											+ "Sentence identifiers do not match!!! " + es.getSentenceIdentifier()
											+ " != " + sentenceId1 + "\nBERT_OUTPUT_LINE: " + bertOutputLine
											+ "\nMETADATA_LINE: " + metadataLine);
						}
//						}
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				throw new IllegalArgumentException("ARRAY INDEX OOB on line: " + bertOutputLine, e);
			}
		}
	}

	protected static String getEntityId(BiolinkAssociation biolinkAssoc, String subjectSpanStr, String documentId,
			String sentence, String sub) {
		String subjectEntityId = DigestUtils
				.sha256Hex(documentId + sentence + sub + subjectSpanStr + biolinkAssoc.getAssociationId());
		return subjectEntityId;
	}

	protected static String getEvidenceId(BiolinkAssociation biolinkAssoc, String subjectSpanStr, String objectSpanStr,
			String documentId, String sentence, String sub, String obj) {
		String evidenceId = DigestUtils.sha256Hex(
				documentId + sentence + sub + subjectSpanStr + obj + objectSpanStr + biolinkAssoc.getAssociationId());
		return evidenceId;
	}

	protected static String getAssertionId(BiolinkAssociation biolinkAssoc, String sub, String obj) {
		String assertionId = DigestUtils.sha256Hex(sub + obj + biolinkAssoc.getAssociationId());
		return assertionId;
	}

	private static List<String> getPredicateCuries(BiolinkAssociation biolinkAssociation) {
		List<String> curies = new ArrayList<String>();
		for (SPO spo : biolinkAssociation.getSpoTriples()) {
			BiolinkPredicate predicate = spo.getPredicate();
			if (predicate == BiolinkPredicate.NO_RELATION_PRESENT) {
				curies.add("false");
			} else {
				curies.add(predicate.getCurie());
			}
		}

		return curies;
	}

//	@DefaultSchema(JavaFieldSchema.class)
	@Data
	public static class EvidenceScoreTableValues implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String evidenceId;
		private final String predicateCurie;
		private final double score;
	}

	public static class EvidenceScoreTableValuesCoder extends Coder<EvidenceScoreTableValues> {

		private static final long serialVersionUID = 1L;
		private static final String SEPARATOR = "|||";

		@Override
		public void encode(EvidenceScoreTableValues val, OutputStream outStream) throws IOException {
			// Dummy encoder, separates first and last names by _
			String serializablePerson = val.getEvidenceId() + SEPARATOR + val.getPredicateCurie() + SEPARATOR
					+ val.getScore();
			outStream.write(serializablePerson.getBytes());
		}

		@Override
		public EvidenceScoreTableValues decode(InputStream inStream) throws CoderException, IOException {
			String serialized = IOUtils.toString(inStream, CharacterEncoding.UTF_8.getCharacterSetName());
			String[] cols = serialized.split("\\|\\|\\|");
			return new EvidenceScoreTableValues(cols[0], cols[1], Double.parseDouble(cols[2]));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() throws NonDeterministicException {
		}
	}

//	@DefaultSchema(JavaFieldSchema.class)
	@Data
	public static class EntityTableValues implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String entityId;
		private final String span;
		private final String coveredText;
	}

	public static class EntityTableValuesCoder extends Coder<EntityTableValues> {

		@Override
		public boolean consistentWithEquals() {
			return true;
		}

		private static final long serialVersionUID = 1L;
		private static final String SEPARATOR = "|||";

		@Override
		public void encode(EntityTableValues val, OutputStream outStream) throws IOException {
			// Dummy encoder, separates first and last names by _
			String serializablePerson = val.getEntityId() + SEPARATOR + val.getSpan() + SEPARATOR
					+ val.getCoveredText();
			outStream.write(serializablePerson.getBytes());
		}

		@Override
		public EntityTableValues decode(InputStream inStream) throws CoderException, IOException {
			String serialized = IOUtils.toString(inStream, CharacterEncoding.UTF_8.getCharacterSetName());
			String[] cols = serialized.split("\\|\\|\\|");
			return new EntityTableValues(cols[0], cols[1], cols[2]);
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() throws NonDeterministicException {
		}
	}

//	@DefaultSchema(JavaFieldSchema.class)
	@Data
	public static class AssertionTableValues implements Serializable {
		private static final long serialVersionUID = 1L;
		/* assertion table */
		private final String assertionId;
		private final String subjectCurie;
		private final String objectCurie;
		private final String associationCurie;

	}

	public static class AssertionTableValuesCoder extends Coder<AssertionTableValues> {

		private static final long serialVersionUID = 1L;
		private static final String SEPARATOR = "|||";

		@Override
		public void encode(AssertionTableValues val, OutputStream outStream) throws IOException {
			// Dummy encoder, separates first and last names by _
			String serializablePerson = val.getAssertionId() + SEPARATOR + val.getSubjectCurie() + SEPARATOR
					+ val.getObjectCurie() + SEPARATOR + val.getAssociationCurie();
			outStream.write(serializablePerson.getBytes());
		}

		@Override
		public AssertionTableValues decode(InputStream inStream) throws CoderException, IOException {
			String serialized = IOUtils.toString(inStream, CharacterEncoding.UTF_8.getCharacterSetName());
			String[] cols = serialized.split("\\|\\|\\|");
			return new AssertionTableValues(cols[0], cols[1], cols[2], cols[3]);
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() throws NonDeterministicException {
		}
	}

//	@DefaultSchema(JavaFieldSchema.class)
	@Data
	public static class EvidenceTableValues implements Serializable {
		private static final long serialVersionUID = 1L;
		/* evidence table */
		private final String evidenceId;
		private final String assertionId;
		private final String documentId;
		private final String sentence;
		private final String subjectEntityId;
		private final String objectEntityId;
		private final String documentZone;
		private final String documentPublicationTypesStr;
		private final int documentYearPublished;
	}

	public static class EvidenceTableValuesCoder extends Coder<EvidenceTableValues> {

		private static final long serialVersionUID = 1L;
		private static final String SEPARATOR = "|||";

		@Override
		public void encode(EvidenceTableValues val, OutputStream outStream) throws IOException {
			// Dummy encoder, separates first and last names by _
			String serializablePerson = val.getEvidenceId() + SEPARATOR + val.getAssertionId() + SEPARATOR
					+ val.getDocumentId() + SEPARATOR + val.getSentence() + SEPARATOR + val.getSubjectEntityId()
					+ SEPARATOR + val.getObjectEntityId() + SEPARATOR + val.getDocumentZone() + SEPARATOR
					+ val.getDocumentPublicationTypesStr() + SEPARATOR + val.getDocumentYearPublished();
			outStream.write(serializablePerson.getBytes());
		}

		@Override
		public EvidenceTableValues decode(InputStream inStream) throws CoderException, IOException {
			String serialized = IOUtils.toString(inStream, CharacterEncoding.UTF_8.getCharacterSetName());
			String[] cols = serialized.split("\\|\\|\\|");
			return new EvidenceTableValues(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7],
					Integer.parseInt(cols[8]));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() throws NonDeterministicException {
		}
	}

//	@Data
//	public static class SqlValues implements Serializable {
//		private static final long serialVersionUID = 1L;
//		/* assertion table */
//		private final String assertionId;
//		private final String subjectCurie;
//		private final String objectCurie;
//		private final String associationCurie;
//
//		/* evidence table */
//		private final String evidenceId;
//		private final String documentId;
//		private final String sentence;
//		private final String subjectEntityId;
//		private final String objectEntityId;
//		private final String documentZone;
//		private final String documentPublicationTypesStr;
//		private final int documentYearPublished;
//
//		/* entity table */
//		private final String subjectSpanStr;
//		private final String objectSpanStr;
//		private final String subjectCoveredText;
//		private final String objectCoveredText;
//
//		private final Map<String, Double> predicateCurieToScoreMap = new HashMap<String, Double>();
//
//		public void addScore(String predicateCurie, double score) {
//			predicateCurieToScoreMap.put(predicateCurie, score);
//		}
//
//		public double getScore(String predicateCurie) {
//			return predicateCurieToScoreMap.get(predicateCurie);
//		}
//	}
}
