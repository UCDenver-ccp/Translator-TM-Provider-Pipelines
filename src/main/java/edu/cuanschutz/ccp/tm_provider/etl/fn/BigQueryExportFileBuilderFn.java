package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Arrays;
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
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.TableKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryLoadBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class BigQueryExportFileBuilderFn extends DoFn<KV<String, Map<DocumentType, String>>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> ANNOTATION_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> IN_SECTION_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> IN_PARAGRAPH_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> IN_SENTENCE_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> IN_CONCEPT_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> RELATION_TABLE_OUTPUT_TAG = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<String> ANNOTATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> IN_SECTION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> IN_PARAGRAPH_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> IN_SENTENCE_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> IN_CONCEPT_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<String> RELATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID = new TupleTag<String>() {
	};

	private final String outputBucket;
	private final PipelineKey pipeline;
	private final String pipelineVersion;
	private final com.google.cloud.Timestamp timestamp;

	public static PCollectionTuple process(PCollection<KV<String, Map<DocumentType, String>>> docIdToAnnotations,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return docIdToAnnotations.apply("Create BigQuery load file",
				ParDo.of(new DoFn<KV<String, Map<DocumentType, String>>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, Map<DocumentType, String>> docIdToAnnotations,
							MultiOutputReceiver out) {

						String docId = docIdToAnnotations.getKey();

						Map<DocumentType, String> docTypeToContent = docIdToAnnotations.getValue();

						try {

							BigQueryLoadBuilder builder = new BigQueryLoadBuilder();
							Map<TableKey, String> bigQueryTables = builder.toBigQueryString(docId, null,
									docTypeToContent);

							out.get(ANNOTATION_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.ANNOTATION)));
							out.get(IN_SECTION_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.IN_SECTION)));
							out.get(IN_PARAGRAPH_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.IN_PARAGRAPH)));
							out.get(IN_SENTENCE_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.IN_SENTENCE)));
							out.get(IN_CONCEPT_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.IN_CONCEPT)));
							out.get(RELATION_TABLE_OUTPUT_TAG)
									.output(KV.of(docId, bigQueryTables.get(TableKey.RELATION)));

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during file download for document ID.", docId, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(ANNOTATION_TABLE_OUTPUT_TAG,
						TupleTagList.of(FAILURE_TAG)
								.and(Arrays.asList(IN_SECTION_TABLE_OUTPUT_TAG, IN_PARAGRAPH_TABLE_OUTPUT_TAG,
										IN_SENTENCE_TABLE_OUTPUT_TAG, IN_CONCEPT_TABLE_OUTPUT_TAG,
										RELATION_TABLE_OUTPUT_TAG))));
	}

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

							BigQueryLoadBuilder builder = new BigQueryLoadBuilder();
							Map<TableKey, String> bigQueryTables = builder.toBigQueryString(docId, null,
									docTypeToContent);

							out.get(ANNOTATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.ANNOTATION));
							out.get(IN_SECTION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.IN_SECTION));
							out.get(IN_PARAGRAPH_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.IN_PARAGRAPH));
							out.get(IN_SENTENCE_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.IN_SENTENCE));
							out.get(IN_CONCEPT_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.IN_CONCEPT));
							out.get(RELATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID)
									.output(bigQueryTables.get(TableKey.RELATION));

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during BigQuery table file gen for document ID.", docId, t, timestamp);
							out.get(FAILURE_TAG).output(failure);
						}

					}
				}).withOutputTags(ANNOTATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID,
						TupleTagList.of(FAILURE_TAG).and(Arrays.asList(IN_SECTION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID,
								IN_PARAGRAPH_TABLE_OUTPUT_TAG_NO_TRACK_DOCID,
								IN_SENTENCE_TABLE_OUTPUT_TAG_NO_TRACK_DOCID, IN_CONCEPT_TABLE_OUTPUT_TAG_NO_TRACK_DOCID,
								RELATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID))));
	}

}
