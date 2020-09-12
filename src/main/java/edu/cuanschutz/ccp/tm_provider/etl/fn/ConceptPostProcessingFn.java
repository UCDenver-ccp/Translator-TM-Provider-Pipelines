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

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
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

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria,
			PCollectionView<Map<String, Set<String>>> extensionToOboMapView,
			PCollectionView<Map<String, String>> prPromotionMapView,
			PCollectionView<Map<String, String>> ncbiTaxonPromotionMapView) {

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
						Map<String, String> ncbitaxonPromotionMap = context.sideInput(ncbiTaxonPromotionMapView);

						try {
							// check to see if all documents are present
							Map<DocumentCriteria, String> docs = statusEntityToText.getValue();
							if (!docs.keySet().equals(requiredDocumentCriteria)) {
								PipelineMain.logFailure(ETL_FAILURE_TAG,
										"Unable to extract sentences due to missing documents for: " + docId
												+ " -- contains (" + statusEntityToText.getValue().size() + ") "
												+ statusEntityToText.getValue().keySet().toString(),
										outputDocCriteria, timestamp, out, docId, null);
							} else {

								Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
										.getDocTypeToContentMap(statusEntity.getDocumentId(),
												statusEntityToText.getValue());
								Map<DocumentType, Collection<TextAnnotation>> docTypeToAnnotsMap = PipelineMain
										.filterConceptAnnotations(docTypeToContentMap);

								Set<TextAnnotation> allAnnots = PipelineMain.spliceValues(docTypeToAnnotsMap.values());

								allAnnots = convertExtensionToObo(allAnnots, extensionToOboMap);
								allAnnots = promoteAnnots(allAnnots, prPromotionMap);
								allAnnots = promoteAnnots(allAnnots, ncbitaxonPromotionMap);

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
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during sentence extraction",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withSideInputs(extensionToOboMapView, prPromotionMapView, ncbiTaxonPromotionMapView)
				.withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * Given a set of annotations and a promotion map that maps from one class to
	 * the class it should be converted to, updated all of the annotations.
	 * 
	 * @param annots
	 * @param promotionMap
	 */
	@VisibleForTesting
	protected static Set<TextAnnotation> promoteAnnots(Set<TextAnnotation> annots, Map<String, String> promotionMap) {

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
