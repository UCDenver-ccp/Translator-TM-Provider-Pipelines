package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConceptAnnotationExportFileBuilderFn
		extends DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, String> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<String> ANNOTATION_IN_BIONLP_OUTPUT_TAG = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> TEXT_OUTPUT_TAG = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return statusEntity2Content.apply("Gather text and concept bionlp to export",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusToContentMap,
							MultiOutputReceiver out) {

						ProcessingStatus status = statusToContentMap.getKey();
						String docId = status.getDocumentId();

						try {
							String documentText = null;
							String annotationsInBionlp = null;
							for (Entry<DocumentCriteria, String> entry : statusToContentMap.getValue().entrySet()) {
								DocumentCriteria dc = entry.getKey();
								if (dc.getDocumentType() == DocumentType.ACTIONABLE_TEXT) {
									documentText = entry.getValue();
								} else if (dc.getDocumentType() == DocumentType.CONCEPT_ALL) {
									annotationsInBionlp = entry.getValue();
								}
							}

							if (documentText != null && annotationsInBionlp != null) {
								// add document separators
								String documentSeparator = String.format("\n================================= %s",
										docId);
								annotationsInBionlp = annotationsInBionlp + documentSeparator;
								documentText = documentText + documentSeparator;

								out.get(ANNOTATION_IN_BIONLP_OUTPUT_TAG).output(annotationsInBionlp);
								out.get(TEXT_OUTPUT_TAG).output(documentText);
							} else {
								throw new IllegalStateException("Exporting concept annotations. Document text is null: "
										+ (documentText == null) + " or concepts_all document is null: "
										+ (annotationsInBionlp == null) + " for document " + docId);
							}

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during concept annotation export.", docId, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(ANNOTATION_IN_BIONLP_OUTPUT_TAG, TupleTagList.of(FAILURE_TAG).and(TEXT_OUTPUT_TAG)));

	}

}
