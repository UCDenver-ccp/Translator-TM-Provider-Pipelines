package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class OgerPostProcessingFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria,
			PCollectionView<Map<String, String>> idToOgerDictEntriesMapView, PipelineKey pipelineKey) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						Map<String, String> idToOgerDictEntriesMap = context.sideInput(idToOgerDictEntriesMapView);

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (PipelineMain.requiredDocumentsArePresent(docs.keySet(), requiredDocumentCriteria,
									pipelineKey, ETL_FAILURE_TAG, docId, outputDocCriteria, timestamp, out)) {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(), docs);

								// this component requires the augmented document text - this text consists of
								// the original document text followed by sentences from the text that have been
								// added to the document in altered form so that they can also be processed by
								// the concept recognition machinery. Specifically, these are sentences that
								// contain abbreviation definitions where the short form part of the definition
								// has been replaced by whitespace. The replacement by whitespace allows for
								// concept recognition matches that may not otherwise occur.
								String augmentedDocumentText = PipelineMain.getAugmentedDocumentText(docs, docId);

								Collection<TextAnnotation> allAnnots = CollectionsUtil
										.consolidate(docTypeToContentMap.values());

								Set<TextAnnotation> nonRedundantAnnots = new HashSet<TextAnnotation>();
								// remove all slots so that duplicate annotations will be condensed. There is a
								// slot for the theme id, e.g., T32, which will prevent duplicate annotations
								// from being condensed in the set -- the clone method creates a clone of the
								// annotation without any slots
								for (TextAnnotation annot : allAnnots) {
									nonRedundantAnnots.add(PipelineMain.clone(annot));
								}

								nonRedundantAnnots = removeSpuriousMatches(nonRedundantAnnots, idToOgerDictEntriesMap);

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

				}).withSideInputs(idToOgerDictEntriesMapView)
				.withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
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
			Map<String, String> idToOgerDictEntriesMap) {
		LevenshteinDistance ld = LevenshteinDistance.getDefaultInstance();
		Set<TextAnnotation> toReturn = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : allAnnots) {
			String id = annot.getClassMention().getMentionName();
			String coveredText = annot.getCoveredText();
			// in the hybrid abbreviation handling, we sometimes get matches that contain
			// consecutive whitespace, e.g., from where the short form portion of an
			// abbreviation was removed in the covered text. Here we condense consecutive
			// spaces into a single space so that the levenshstein distance calculation
			// below isn't adversely affected.
			coveredText = coveredText.replaceAll("\\s+", " ");

			// if the covered text is just digits and punctuation, then we will exclude it
			if (isDigitsAndPunctOnly(coveredText)) {
				continue;
			}

			if (idToOgerDictEntriesMap.containsKey(id)) {
				String dictEntries = idToOgerDictEntriesMap.get(id);
				for (String dictEntry : dictEntries.split("\\|")) {
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
//								System.out.println("Excluding due to closest match has number suffix: " + annot.getAggregateSpan().toString() + " "
//										+ coveredText + " -- " + annot.getClassMention().getMentionName());
							}
						}

						if (!exclude) {
							toReturn.add(annot);
						}
					}
//					System.out.println(String.format("%s -- %s == %d %f", coveredText, dictEntry, dist, percentChange));
				}
			} else {
				// because we have had to split the idToOgerDictEntriesMap into multiple pieces,
				// not all concept IDs are processed during a single run b/c not all concept IDs
				// will be represented in the map. So, if we don't find an ID in the map, we
				// simply pass the annotation through. This annotation was either already
				// processed during a previous run, or will be processed during a subsequent
				// run.
				toReturn.add(annot);
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

}
