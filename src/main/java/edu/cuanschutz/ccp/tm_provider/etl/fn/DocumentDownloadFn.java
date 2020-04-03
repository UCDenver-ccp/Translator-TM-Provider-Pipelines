package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;

public class DocumentDownloadFn extends DoFn<String, KV<String, Map<String, String>>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, Map<DocumentType, String>>> OUTPUT_TAG = new TupleTag<KV<String, Map<DocumentType, String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<Map<DocumentType, String>> OUTPUT_TAG_NO_DOC_ID = new TupleTag<Map<DocumentType, String>>() {
	};

	public static PCollectionTuple process(PCollection<String> documentIds, PipelineKey pipeline,
			String pipelineVersion, com.google.cloud.Timestamp timestamp, List<DocumentCriteria> documentCriteria) {

		return documentIds.apply("Download files for document ID",
				ParDo.of(new DoFn<String, KV<String, Map<DocumentType, String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element String docId, MultiOutputReceiver out) {
						DatastoreDocumentUtil util = new DatastoreDocumentUtil();
						try {
							Map<DocumentType, String> typeToContentMap = util.getDocumentTypeToContent(docId,
									documentCriteria);
							out.get(OUTPUT_TAG).output(KV.of(docId, typeToContentMap));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(pipeline, pipelineVersion,
									"Failure during file download for document ID. (Document type listed is non-specific. "
											+ "Could have been a different type that caused the error.",
									docId, DocumentType.TEXT, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));
	}

	public static PCollectionTuple processNoTrackDocIds(PCollection<String> documentIds, PipelineKey pipeline,
			String pipelineVersion, com.google.cloud.Timestamp timestamp, List<DocumentCriteria> documentCriteria) {

		return documentIds.apply("Download files for document ID",
				ParDo.of(new DoFn<String, Map<DocumentType, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element String docId, MultiOutputReceiver out) {
						DatastoreDocumentUtil util = new DatastoreDocumentUtil();
						try {
							Map<DocumentType, String> typeToContentMap = util.getDocumentTypeToContent(docId,
									documentCriteria);
							out.get(OUTPUT_TAG_NO_DOC_ID).output(typeToContentMap);
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(pipeline, pipelineVersion,
									"Failure during file download for document ID. (Document type listed is non-specific. "
											+ "Could have been a different type that caused the error.",
									docId, DocumentType.TEXT, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(OUTPUT_TAG_NO_DOC_ID, TupleTagList.of(FAILURE_TAG)));
	}

}
