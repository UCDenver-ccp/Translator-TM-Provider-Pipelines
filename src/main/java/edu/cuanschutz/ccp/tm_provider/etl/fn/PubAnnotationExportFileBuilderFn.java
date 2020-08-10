package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.PubAnnotationFormatter;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class PubAnnotationExportFileBuilderFn extends DoFn<KV<String, Map<DocumentType, String>>, String> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<String> JSON_OUTPUT_TAG = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	private final String outputBucket;
	private final PipelineKey pipeline;
	private final String pipelineVersion;
	private final com.google.cloud.Timestamp timestamp;

	public static PCollectionTuple processNoTrackDocIds(
			PCollection<KV<String, Map<DocumentType, String>>> docIdToAnnotations, DocumentCriteria outputDocCriteria,
			com.google.cloud.Timestamp timestamp) {

		return docIdToAnnotations.apply("Create BigQuery load file",
				ParDo.of(new DoFn<KV<String, Map<DocumentType, String>>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, Map<DocumentType, String>> docIdToAnnotations,
							MultiOutputReceiver out) {

						String docId = docIdToAnnotations.getKey();

						Map<DocumentType, String> docTypeToContent = docIdToAnnotations.getValue();

						try {

							PubAnnotationFormatter builder = new PubAnnotationFormatter();
							String pubAnnotJson = builder.toPubAnnotationJson(docId, null, docTypeToContent);

							out.get(JSON_OUTPUT_TAG).output(pubAnnotJson);

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during BigQuery table file gen for document ID.", docId, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(JSON_OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

	}

}
