package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.string.StringUtil;

/**
 * Utility for loading documents that are already plain text
 * 
 * Outputs four {@link PCollection} objects
 * <ul>
 * <li>mapping document ID to plain text</li>
 * <li>a log of any failures</li>
 * <li>a status object that indicates which jobs still need processing, e.g.
 * dependency parse, etc.</li>
 * </ul>
 *
 */
public class ExtractContentFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	private final static Logger LOGGER = Logger.getLogger(ExtractContentFn.class.getName());

	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> contentTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToText,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp, String fileSuffix,
			String collection, ProcessingStatusFlag targetProcessingStatusFlag) {

		return docIdToText.apply("Extract file content; create status if TEXT",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToContent, MultiOutputReceiver out) {
						String fileId = docIdToContent.getKey();
						String content = docIdToContent.getValue();

						try {
							// the document id is parsed from the file name
							String docId = (fileId.contains("/")) ? fileId.substring(fileId.lastIndexOf("/") + 1)
									: fileId;
							docId = (docId.endsWith(fileSuffix)) ? StringUtil.removeSuffix(docId, fileSuffix) : docId;

							/*
							 * divide the document content into chunks if necessary so that each chunk is
							 * under the DataStore byte length threshold
							 */
							List<String> chunkedContent = PipelineMain.chunkContent(content);
							out.get(contentTag).output(KV.of(docId, chunkedContent));

							if (targetProcessingStatusFlag == ProcessingStatusFlag.TEXT_DONE) {
								/*
								 * output a new {@link ProcessingStatus} for the document if the target
								 * processing status flag is TEXT_DONE. If it is TEXT_DONE we assume this to be
								 * a new document being added for further processing. Alternatively this is
								 * content related to a document that (presumably) is already in the Datastore,
								 * e.g. a section annotations file for a document that was previously loaded.
								 */
								ProcessingStatus status = new ProcessingStatus(docId);
								status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
//										chunkedContent.size());
								if (collection != null) {
									status.addCollection(collection);
								}
								out.get(processingStatusTag).output(status);
							}

						} catch (Throwable t) {
							LOGGER.log(Level.SEVERE, "Error while extracting content.", t);
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure saving file. Likely encoding issue with input document.", fileId, t,
									timestamp);
							out.get(etlFailureTag).output(failure);
						}

					}
				}).withOutputTags(contentTag, TupleTagList.of(etlFailureTag).and(processingStatusTag)));
	}

}
