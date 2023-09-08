package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * This class was originally designed to create a version of the plain txt of a
 * document that filters text we do not want to process, e.g., the reference
 * section. We refer to the filtered text as "unactionable".
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class FilterUnactionableTextFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	private static final Set<String> UNACTIONABLE_TOP_LEVEL_SECTION_LABELS = new HashSet<String>(Arrays.asList("REF",
			"SUPPL", "COMP_INT", "AUTH_CONT", "ABBR", "ACK_FUND", "APPENDIX", "KEYWORD", "REVIEW_INFO", "reference"));

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> FILTERED_TEXT_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<String> TOP_LEVEL_SECTION_TAG = new TupleTag<String>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content,
			DocumentCriteria outputDocCriteria, Set<DocumentCriteria> requiredDocumentCriteria,
			com.google.cloud.Timestamp timestamp, PipelineKey pipelineKey) {

		return statusEntity2Content.apply("filter unactionable text", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (PipelineMain.requiredDocumentsArePresent(docs.keySet(), requiredDocumentCriteria,
									pipelineKey, ETL_FAILURE_TAG, docId, outputDocCriteria, timestamp, out)) {
//							if (!PipelineMain.fulfillsRequiredDocumentCriteria(docs.keySet(),
//									requiredDocumentCriteria)) {
//								PipelineMain.logFailure(ETL_FAILURE_TAG,
//										"Unable to complete filter unactionable text due to missing annotation documents for: "
//												+ docId + " -- contains (" + docs.size() + ") "
//												+ docs.keySet().toString(),
//										outputDocCriteria, timestamp, out, docId, null);
//							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(), docs);

								Collection<TextAnnotation> sectionAnnots = docTypeToContentMap
										.get(DocumentType.SECTIONS);

								String documentText = PipelineMain.getDocumentText(docs, docId);

								String augmentedDocText = filterUnactionableText(documentText, sectionAnnots, out);

								List<String> chunkedAugDocText = PipelineMain.chunkContent(augmentedDocText);
								out.get(FILTERED_TEXT_TAG).output(KV.of(statusEntity, chunkedAugDocText));
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during document text augmentation. ",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withOutputTags(FILTERED_TEXT_TAG, TupleTagList.of(ETL_FAILURE_TAG).and(TOP_LEVEL_SECTION_TAG)));
	}

	/**
	 * Removes unactionable text from the bottom of the document.
	 * 
	 * @param documentText
	 * @param sectionAnnots
	 * @param out
	 * @return
	 */
	protected static String filterUnactionableText(String documentText, Collection<TextAnnotation> sectionAnnots,
			MultiOutputReceiver out) {

		List<TextAnnotation> topLevelSections = getTopLevelSections(sectionAnnots);
		Collections.sort(topLevelSections, TextAnnotation.BY_SPAN());

		Set<String> topLevelSectionNames = getTopLevelSectionNames(topLevelSections);
		if (out != null) {
			out.get(TOP_LEVEL_SECTION_TAG).output(CollectionsUtil.createDelimitedString(topLevelSectionNames, "\n"));
		}

		// working from the bottom, exclude sections that have been deemed unactionable,
		// e.g., the references

		int endIndex = documentText.length();
		for (int i = topLevelSections.size() - 1; i >= 0; i--) {
			TextAnnotation section = topLevelSections.get(i);
			String sectionName = section.getClassMention().getMentionName();
			if (UNACTIONABLE_TOP_LEVEL_SECTION_LABELS.contains(sectionName)) {
				endIndex = section.getAnnotationSpanStart();
			} else {
				// for now, we only filter a contiguous section of unactionable text at the
				// bottom of the document. We stop removing text once an actionable section is
				// observed.
				break;
			}
		}

		String filteredDocText = documentText.substring(0, endIndex);

		return filteredDocText;

	}

	private static Set<String> getTopLevelSectionNames(List<TextAnnotation> topLevelSections) {
		Set<String> set = new HashSet<String>();
		for (TextAnnotation annot : topLevelSections) {
			set.add(annot.getClassMention().getMentionName());
		}
		return set;
	}

	/**
	 * This method removes sections that are not top-level
	 * 
	 * @param sectionAnnots
	 * @return
	 */
	protected static List<TextAnnotation> getTopLevelSections(Collection<TextAnnotation> sectionAnnots) {
		List<TextAnnotation> sortedSections = new ArrayList<TextAnnotation>(sectionAnnots);
		Collections.sort(sortedSections, TextAnnotation.BY_SPAN());

		Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();

		for (int i = 0; i < sortedSections.size(); i++) {
			TextAnnotation section = sortedSections.get(i);
			if (toRemove.contains(section)) {
				// if this annotation has already been marked for removal, then we don't need to
				// process it
				continue;
			}
			for (int j = i + 1; j < sortedSections.size(); j++) {
				TextAnnotation nextSection = sortedSections.get(j);
				if (section.getAnnotationSpanEnd() < nextSection.getAnnotationSpanStart()) {
					// if the end of the current span is < the start of the next span, then there is
					// no overlap, so no need to keep checking
					continue;
				}
				if (section.getAggregateSpan().containsSpan(nextSection.getAggregateSpan())) {
					toRemove.add(nextSection);
				} else if (nextSection.getAggregateSpan().containsSpan(section.getAggregateSpan())) {
					toRemove.add(section);
				}

			}
		}

		sortedSections.removeAll(toRemove);
		return sortedSections;
	}

}
