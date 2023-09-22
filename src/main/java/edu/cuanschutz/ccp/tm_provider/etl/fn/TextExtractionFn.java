package edu.cuanschutz.ccp.tm_provider.etl.fn;

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
import edu.cuanschutz.ccp.tm_provider.etl.TextExtractionPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Returns KV pairs mapping document ID to document text where the document text
 * has been prepended with a comment field indicating the document ID, e.g. <br>
 * ###C: DOCUMENT_ID\t[document_id]
 * 
 * The text is also prepended with a comment field listing the document
 * collections to which the document belongs, e.g. <br>
 * ###C: DOCUMENT_COLLECTIONS\tcollection1|collection2|collection3
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class TextExtractionFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, String>> EXTRACTED_TEXT_TAG = new TupleTag<KV<ProcessingStatus, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	/**
	 * @param statusEntityToText
	 * @param outputDocCriteria
	 * @param timestamp
	 * @param inputDocCriteria
	 * @return
	 */
	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> inputDocCriteria) {

		return statusEntityToText.apply("Exporting text",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();
						Set<String> collections = statusEntity.getCollections();
						String docCollectionsStr = CollectionsUtil.createDelimitedString(collections,
								TextExtractionPipeline.DOCUMENT_COLLECTIONS_DELIMITER);

						try {
							String documentText = PipelineMain.getDocumentText(statusEntityToText.getValue(), docId);
							if (documentText == null) {
								PipelineMain.logFailure(ETL_FAILURE_TAG, "Unable to extract text for: " + docId,
										outputDocCriteria, timestamp, out, docId, null);
							} else {
								/*
								 * Prepend the document text with comment lines. One for the document ID, and
								 * one for the document collections.
								 */
								String docIdCommentLine = TextExtractionPipeline.DOCUMENT_ID_COMMENT_PREFIX + docId
										+ "\n";
								String docCollectionsCommentLine = TextExtractionPipeline.DOCUMENT_COLLECTIONS_COMMENT_PREFIX
										+ docCollectionsStr + "\n";
								documentText = docIdCommentLine + docCollectionsCommentLine + "\n" + documentText
										+ "\n";

								out.get(EXTRACTED_TEXT_TAG).output(KV.of(statusEntity, documentText));
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during text extraction",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withOutputTags(EXTRACTED_TEXT_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

}
