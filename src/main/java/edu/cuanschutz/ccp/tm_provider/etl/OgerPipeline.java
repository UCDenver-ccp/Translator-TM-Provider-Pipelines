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
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.OgerFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
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
		String getOgerServiceUri();

		void setOgerServiceUri(String value);

		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		ProcessingStatusFlag getTargetProcessStatusFlag();

		void setTargetProcessStatusFlag(ProcessingStatusFlag flag);

		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
		DocumentType getTargetDocumentType();

		void setTargetDocumentType(DocumentType type);

		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
		DocumentFormat getTargetDocumentFormat();

		void setTargetDocumentFormat(DocumentFormat type);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getInputPipelineKey();

		void setInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getInputPipelineVersion();

		void setInputPipelineVersion(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		LOGGER.log(Level.INFO, String.format("Running OGER pipeline for concept: ", options.getTargetDocumentType()));

		URI ogerServiceUri = URI.create(options.getOgerServiceUri());
		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessStatusFlag();
		DocumentType targetDocumentType = options.getTargetDocumentType();

		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		/*
		 * The pipeline will process each triple (URI, ProcessingStatusFlag,
		 * DocumentType).
		 */
		LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- %s",
				targetProcessingStatusFlag.name(), targetDocumentType.name(), ogerServiceUri));

		/*
		 * The OGER pipeline requires plain text documents, hence the type=TEXT and
		 * format=TEXT below. However, the selection of documents can be adjusted by
		 * specifying different pipeline keys and pipeline versions. These are set in
		 * the options for this pipeline.
		 */
		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getInputPipelineKey(), options.getInputPipelineVersion());
		PCollection<KV<String, String>> docId2Content = PipelineMain.getDocId2Content(inputTextDocCriteria,
				options.getProject(), p, targetProcessingStatusFlag, requiredProcessStatusFlags);

		DocumentCriteria outputDocCriteria = new DocumentCriteria(options.getTargetDocumentType(),
				options.getTargetDocumentFormat(), PIPELINE_KEY, pipelineVersion);
		PCollectionTuple output = OgerFn.process(docId2Content, ogerServiceUri.toString(), outputDocCriteria,
				timestamp);

		PCollection<KV<String, List<String>>> docIdToAnnotation = output.get(OgerFn.ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(OgerFn.ETL_FAILURE_TAG);

		/* store the serialized annotation document in Cloud Datastore */
		docIdToAnnotation.apply("annotation->document_entity", ParDo.of(new DocumentToEntityFn(outputDocCriteria)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn())).apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollection<KV<String, String>> successStatus = DatastoreProcessingStatusUtil
				.getSuccessStatus(docId2Content.apply(Keys.<String>create()), failures, targetProcessingStatusFlag);

		List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
		statusList.add(successStatus);
		DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);

		p.run().waitUntilFinish();
	}

//	/**
//	 * Store the annotation files, any failures, and update the
//	 * targetProcessingStatusFlag to true for those documents that did not result in
//	 * a failure.
//	 * 
//	 * @param pipelineVersion
//	 * @param options
//	 * @param targetProcessStatusFlag
//	 * @param documentType
//	 * @param output
//	 * @param processedDocIds
//	 * @return
//	 */
//
//	private static PCollection<KV<String, String>> logResults(PCollection<String> processedDocIds,
//			String pipelineVersion, String projectId, ProcessingStatusFlag targetProcessStatusFlag, DocumentCriteria dc,
//			PCollectionTuple output) {
//		/*
//		 * Processing of the plain text with OGER results in 1) a PCollection mapping
//		 * document ID to the extracted annotations serialized in BioNLP format. 2) a
//		 * PCollection logging any errors encountered during the concept recognition
//		 * process.
//		 */
//
//		PCollection<KV<String, List<String>>> docIdToAnnotation = output.get(OgerFn.ANNOTATIONS_TAG);
//		PCollection<EtlFailureData> failures = output.get(OgerFn.ETL_FAILURE_TAG);
//
//		/* store the serialized annotation document in Cloud Datastore */
//		docIdToAnnotation.apply("annotation->document_entity", ParDo.of(new DocumentToEntityFn(dc)))
//				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(projectId));
//
//		/* store the failures for this pipeline in Cloud Datastore */
//		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn())).apply("failure_entity->datastore",
//				DatastoreIO.v1().write().withProjectId(projectId));
//
//		return DatastoreProcessingStatusUtil.getSuccessStatus(processedDocIds, failures, targetProcessStatusFlag);
//	}

}
