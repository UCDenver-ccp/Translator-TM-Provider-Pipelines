package edu.cuanschutz.ccp.tm_provider.etl;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.OgerFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * This Apache Beam pipeline processes documents with the OGER concept
 * recognition service reached via HTTP POST. Input is plain text; Output is
 * concept annotations in BioNLP format.
 */
public class OgerPipeline {

	private final static Logger LOGGER = Logger.getLogger(OgerPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.OGER;

	public interface Options extends DataflowPipelineOptions {
		@Description("URI OGER services; pipe-delimited list")
		String getOgerServiceUris();

		void setOgerServiceUris(String value);

		@Description("The targetProcessingStatusFlags should align with the concept type served by the OGER service URI; pipe-delimited list")
		String getTargetProcessStatusFlags();

		void setTargetProcessStatusFlags(String flag);

		@Description("The targetDocumentTypes should also align with the concept type served by the OGER service URI; pipe-delimited list")
		String getTargetDocumentTypes();

		void setTargetDocumentTypes(String type);
	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		LOGGER.log(Level.INFO, String.format("Running OGER pipeline for concepts: ", options.getTargetDocumentTypes()));

		List<URI> ogerServiceUris = extractUris(options.getOgerServiceUris());
		List<ProcessingStatusFlag> processingStatusFlags = extractProcessingStatusFlags(
				options.getTargetProcessStatusFlags());
		List<DocumentType> targetDocumentTypes = extractDocumentTypes(options.getTargetDocumentTypes());

		validate(ogerServiceUris, processingStatusFlags, targetDocumentTypes);

		Pipeline p = Pipeline.create(options);
		DatastoreProcessingStatusUtil statusUtil = new DatastoreProcessingStatusUtil();
		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
		/*
		 * The pipeline will process each triple (URI, ProcessingStatusFlag,
		 * DocumentType).
		 */
		for (int i = 0; i < processingStatusFlags.size(); i++) {
			ProcessingStatusFlag targetProcessStatusFlag = processingStatusFlags.get(i);
			DocumentType documentType = targetDocumentTypes.get(i);
			URI ogerServiceUri = ogerServiceUris.get(i);
			LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- %s",
					targetProcessStatusFlag.name(), documentType.name(), ogerServiceUri));

			PCollection<KV<String, String>> docId2Content = getDocumentsToProcess(pipelineVersion, p, statusUtil,
					requiredProcessStatusFlags, targetProcessStatusFlag, documentType);

			PCollectionTuple output = OgerFn.process(docId2Content, ogerServiceUri.toString(), PIPELINE_KEY,
					pipelineVersion, documentType, timestamp);

			PCollection<KV<String, String>> docIdToStatusToUpdate = logResults(
					docId2Content.apply(Keys.<String>create()), pipelineVersion, options.getProject(),
					targetProcessStatusFlag, documentType, output);

			statusList.add(docIdToStatusToUpdate);
		}

		// update the status for documents that were successfully processed. To the
		// update in batch, e.g one update per document.
		DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);

		p.run().waitUntilFinish();
	}

	/**
	 * Store the annotation files, any failures, and update the
	 * targetProcessingStatusFlag to true for those documents that did not result in
	 * a failure.
	 * 
	 * @param pipelineVersion
	 * @param options
	 * @param targetProcessStatusFlag
	 * @param documentType
	 * @param output
	 * @param processedDocIds
	 * @return
	 */

	private static PCollection<KV<String, String>> logResults(PCollection<String> processedDocIds,
			String pipelineVersion, String projectId, ProcessingStatusFlag targetProcessStatusFlag,
			DocumentType documentType, PCollectionTuple output) {
		/*
		 * Processing of the plain text with OGER results in 1) a PCollection mapping
		 * document ID to the extracted annotations serialized in BioNLP format. 2) a
		 * PCollection logging any errors encountered during the concept recognition
		 * process.
		 */

		PCollection<KV<String, String>> docIdToAnnotation = output.get(OgerFn.ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(OgerFn.ETL_FAILURE_TAG);

		/* store the serialized annotation document in Cloud Datastore */
		docIdToAnnotation
				.apply("annotation->document_entity",
						ParDo.of(new DocumentToEntityFn(documentType, DocumentFormat.BIONLP, PIPELINE_KEY,
								pipelineVersion)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(projectId));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn())).apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(projectId));

		return DatastoreProcessingStatusUtil.getSuccessStatus(processedDocIds, failures, targetProcessStatusFlag);
	}

	/**
	 * Queries Cloud Datastore to find documents that have the
	 * targetProcessingStatusFlag = false, and that also have any required
	 * prerequisite processing already complete.
	 * 
	 * @param pipelineVersion
	 * @param p
	 * @param statusUtil
	 * @param requiredProcessStatusFlags
	 * @param targetProcessStatusFlag
	 * @param documentType
	 * @return
	 */
	private static PCollection<KV<String, String>> getDocumentsToProcess(String pipelineVersion, Pipeline p,
			DatastoreProcessingStatusUtil statusUtil, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			ProcessingStatusFlag targetProcessStatusFlag, DocumentType documentType) {
		// query Cloud Datastore to find document IDs in need of processing
		List<String> documentIdsToProcess = statusUtil.getDocumentIdsInNeedOfProcessing(targetProcessStatusFlag,
				requiredProcessStatusFlags);

		LOGGER.log(Level.INFO, String.format("Pipeline: %s-%s, %d documents to process...", PIPELINE_KEY.name(),
				documentType.name(), documentIdsToProcess.size()));

		// query for the document content for those document IDs - Note that the plain
		// text files are generated by the bioc-to-text pipeline
		List<KV<String, String>> documentIdToContentList = new DatastoreDocumentUtil().getDocumentIdToContentList(
				documentIdsToProcess, DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				pipelineVersion);

		LOGGER.log(Level.INFO, String.format("Pipeline: %s-%s, %d documents retrieved...", PIPELINE_KEY.name(),
				documentType.name(), documentIdToContentList.size()));

		// create initial PCollection
		PCollection<KV<String, String>> docId2Content = p.apply(
				Create.of(documentIdToContentList).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
		return docId2Content;
	}

	/**
	 * ensure an equal number of values in each parameter list
	 * 
	 * @param ogerServiceUris
	 * @param processingStatusFlags
	 * @param targetDocumentTypes
	 */
	private static void validate(List<URI> ogerServiceUris, List<ProcessingStatusFlag> processingStatusFlags,
			List<DocumentType> targetDocumentTypes) {
		int size1 = ogerServiceUris.size();
		int size2 = processingStatusFlags.size();
		int size3 = targetDocumentTypes.size();

		if (!(size1 == size2 && size1 == size3)) {
			throw new IllegalArgumentException(
					String.format("Detected mismatch in number of input arguments. Please adjust and re-try. "
							+ "service-uris=%d; status-flags=%d; document-types=%d", size1, size2, size3));
		}

	}

	/**
	 * @param targetDocumentTypes
	 * @return a list of {@link DocumentType} given the input pipe-delimited list of
	 *         document type names
	 */
	private static List<DocumentType> extractDocumentTypes(String targetDocumentTypes) {
		List<DocumentType> types = new ArrayList<DocumentType>();
		String[] toks = targetDocumentTypes.split("\\|");
		for (String tok : toks) {
			types.add(DocumentType.valueOf(tok.toUpperCase()));
		}
		return types;
	}

	/**
	 * @param targetProcessStatusFlags
	 * @return a list of {@link ProcessingStatusFlag} given the input pipe-delimited
	 *         list of processing status flag names
	 */
	private static List<ProcessingStatusFlag> extractProcessingStatusFlags(String targetProcessStatusFlags) {
		List<ProcessingStatusFlag> flags = new ArrayList<ProcessingStatusFlag>();
		String[] toks = targetProcessStatusFlags.split("\\|");
		for (String tok : toks) {
			flags.add(ProcessingStatusFlag.valueOf(tok.toUpperCase()));
		}
		return flags;
	}

	/**
	 * @param ogerServiceUris
	 * @return a list of {@link URI} given the input pipe-delimited list of URI
	 *         strings
	 */
	private static List<URI> extractUris(String ogerServiceUris) {
		List<URI> flags = new ArrayList<URI>();
		String[] toks = ogerServiceUris.split("\\|");
		for (String tok : toks) {
			flags.add(URI.create(tok));
		}
		return flags;
	}

}
