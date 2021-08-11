package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.CrfNerFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * This Apache Beam pipeline processes documents with the OGER concept
 * recognition service reached via HTTP POST. Input is plain text; Output is
 * concept annotations in BioNLP format.
 */
public class CrfNerPipeline {

	private final static Logger LOGGER = Logger.getLogger(CrfNerPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.CRF;

	public interface Options extends DataflowPipelineOptions {
		@Description("URI for the CRF entity recognition service")
		String getCrfServiceUri();

		void setCrfServiceUri(String value);

		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
		DocumentType getTargetDocumentType();

		void setTargetDocumentType(DocumentType type);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getInputSentencePipelineKey();

		void setInputSentencePipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getInputSentencePipelineVersion();

		void setInputSentencePipelineVersion(String value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		DocumentType targetDocumentType = options.getTargetDocumentType();
		LOGGER.log(Level.INFO, String.format("Running CRF pipeline for: ", targetProcessingStatusFlag.name()));

		Pipeline p = Pipeline.create(options);

		/*
		 * require that the documents have a plain text version and that they have been
		 * segmented into sentences
		 */
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.SENTENCE_DONE);

		/*
		 * The pipeline will process each triple (URI, ProcessingStatusFlag,
		 * DocumentType).
		 */
		LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- %s",
				targetProcessingStatusFlag.name(), targetDocumentType.name(), options.getCrfServiceUri()));

		/*
		 * The CRF pipeline requires sentences and the document text.
		 */
		Set<DocumentCriteria> inputDocCriteria = CollectionsUtil
				.createSet(new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
						options.getInputSentencePipelineKey(), options.getInputSentencePipelineVersion()));
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(options.getTargetDocumentType(),
				DocumentFormat.BIONLP, PIPELINE_KEY, pipelineVersion);
		PCollectionTuple output = CrfNerFn.process(statusEntity2Content, options.getCrfServiceUri(), outputDocCriteria,
				timestamp);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAnnotation = output.get(CrfNerFn.ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(CrfNerFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToAnnotation.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToAnnotation);
		nonredundantDocIdToAnnotations
				.apply("annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(outputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();
	}

}
