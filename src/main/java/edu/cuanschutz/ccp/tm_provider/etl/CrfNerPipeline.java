package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.MultithreadedServiceCalls;
import edu.cuanschutz.ccp.tm_provider.etl.fn.CrfNerFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * This Apache Beam pipeline processes documents with the OGER concept
 * recognition service reached via HTTP POST. Input is plain text; Output is
 * concept annotations in BioNLP format.
 */
public class CrfNerPipeline {

//	private final static Logger LOGGER = Logger.getLogger(CrfNerPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.CRF;

	public interface Options extends DataflowPipelineOptions {
		@Description("URIs for the CRAFT CRF entity recognition service")
		@Required
		String getCraftCrfServiceUri();

		void setCraftCrfServiceUri(String value);

		@Description("URIs for the NLM DISEASE CRF entity recognition service")
		@Required
		String getNlmDiseaseCrfServiceUri();

		void setNlmDiseaseCrfServiceUri(String value);

//		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
//		ProcessingStatusFlag getTargetProcessingStatusFlag();
//
//		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);
//
//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentType getTargetDocumentType();
//
//		void setTargetDocumentType(DocumentType type);

		@Description("This pipeline key will be used to select the input sentence bionlp documents that will be processed")
		@Required
		PipelineKey getInputSentencePipelineKey();

		void setInputSentencePipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input sentence bionlp documents that will be processed")
		@Required
		String getInputSentencePipelineVersion();

		void setInputSentencePipelineVersion(String value);

		@Description("This pipeline key will be used to select the input augmented sentence bionlp documents that will be processed")
		@Required
		PipelineKey getAugmentedSentencePipelineKey();

		void setAugmentedSentencePipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input augmented sentence bionlp documents that will be processed")
		@Required
		String getAugmentedSentencePipelineVersion();

		void setAugmentedSentencePipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("Version that will be assigned to the Datastore documents created by this pipeline")
		@Required
		String getOutputPipelineVersion();

		void setOutputPipelineVersion(String value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
//		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.CRF_DONE;
//		DocumentType targetDocumentType = options.getTargetDocumentType();
//		LOGGER.log(Level.INFO, String.format("Running CRF pipeline for: ", targetProcessingStatusFlag.name()));

		Pipeline p = Pipeline.create(options);

		/*
		 * require that the documents have a plain text version and that they have been
		 * segmented into sentences
		 */
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.SENTENCE_DONE);

//		/*
//		 * The pipeline will process each triple (URI, ProcessingStatusFlag,
//		 * DocumentType).
//		 */
//		LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- %s",
//				targetProcessingStatusFlag.name(), targetDocumentType.name(), options.getCrfServiceUri()));

		/*
		 * The CRF pipeline requires sentences and the document text.
		 */
		Set<DocumentCriteria> inputDocCriteria = CollectionsUtil.createSet(
				new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
						options.getInputSentencePipelineKey(), options.getInputSentencePipelineVersion()),
				new DocumentCriteria(DocumentType.AUGMENTED_SENTENCE, DocumentFormat.BIONLP,
						options.getAugmentedSentencePipelineKey(), options.getAugmentedSentencePipelineVersion()));
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria craftCrfOutputDocCriteria = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP,
				PIPELINE_KEY, options.getOutputPipelineVersion());
		DocumentCriteria nlmDiseaseCrfOutputDocCriteria = new DocumentCriteria(DocumentType.CRF_NLMDISEASE,
				DocumentFormat.BIONLP, PIPELINE_KEY, options.getOutputPipelineVersion());

		// note: the craft document criteria is used as the error document criteria
		// note: enabling multithreaded service calls results in out of memory errors
		PCollectionTuple output = CrfNerFn.process(statusEntity2Content, options.getCraftCrfServiceUri(),
				options.getNlmDiseaseCrfServiceUri(), craftCrfOutputDocCriteria, timestamp,
				MultithreadedServiceCalls.DISABLED);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToCraftAnnotation = output
				.get(CrfNerFn.CRAFT_NER_ANNOTATIONS_TAG);
		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToNlmDiseaseAnnotation = output
				.get(CrfNerFn.NLMDISEASE_NER_ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(CrfNerFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToCraftAnnotation.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the serialized CRAFT NER annotation document content in Cloud Datastore
		 * - deduplication is necessary to avoid Datastore non-transactional commit
		 * errors
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToCraftAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToCraftAnnotation);
		nonredundantDocIdToCraftAnnotations
				.apply("craft annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(craftCrfOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("craft annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the serialized NLM DISEASE NER annotation document content in Cloud
		 * Datastore - deduplication is necessary to avoid Datastore non-transactional
		 * commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToNlmDiseaseAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToNlmDiseaseAnnotation);
		nonredundantDocIdToNlmDiseaseAnnotations
				.apply("nlmdisease annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(nlmDiseaseCrfOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("nlmdisease annot_entity->datastore",
						DatastoreIO.v1().write().withProjectId(options.getProject()));

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
