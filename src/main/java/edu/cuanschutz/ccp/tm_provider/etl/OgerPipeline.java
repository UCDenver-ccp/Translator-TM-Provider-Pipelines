package edu.cuanschutz.ccp.tm_provider.etl;

import java.net.URI;
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
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.OgerFn;
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
public class OgerPipeline {

//	private final static Logger LOGGER = Logger.getLogger(OgerPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.OGER;

	public interface Options extends DataflowPipelineOptions {
		@Description("URI for case sensitive OGER services; pipe-delimited list")
		@Required
		String getCsOgerServiceUri();

		void setCsOgerServiceUri(String value);

		@Description("URI for case-insentive min norm OGER services; pipe-delimited list")
		@Required
		String getCiminOgerServiceUri();

		void setCiminOgerServiceUri(String value);

		@Description("URI for case-insensitive max norm OGER services; pipe-delimited list")
		@Required
		String getCimaxOgerServiceUri();

		void setCimaxOgerServiceUri(String value);

//		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
//		ProcessingStatusFlag getTargetProcessingStatusFlag();
//
//		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);
//
//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentType getTargetDocumentType();
//
//		void setTargetDocumentType(DocumentType type);
//
//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentFormat getTargetDocumentFormat();
//
//		void setTargetDocumentFormat(DocumentFormat type);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getTextPipelineKey();

		void setTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getTextPipelineVersion();

		void setTextPipelineVersion(String value);

		@Description("This pipeline key will be used to select the input augmented text documents that will be processed")
		@Required
		PipelineKey getAugmentedTextPipelineKey();

		void setAugmentedTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input augmented text documents that will be processed")
		@Required
		String getAugmentedTextPipelineVersion();

		void setAugmentedTextPipelineVersion(String value);

		@Description("This pipeline version will be used as part of the output document key/name")
		@Required
		String getOutputPipelineVersion();

		void setOutputPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

//		@Description("The output type expected to be returned by the Oger service. If OGER is run without BioBert, "
//				+ "then it returns TSV, otherwise it returns PubAnnotation formatted output.")
//		OgerOutputType getOgerOutputType();
//
//		void setOgerOutputType(OgerOutputType value);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
//		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		LOGGER.log(Level.INFO, String.format("Running OGER pipeline for concept: ", options.getTargetDocumentType()));

		URI csOgerServiceUri = URI.create(options.getCsOgerServiceUri());
		URI ciminOgerServiceUri = URI.create(options.getCiminOgerServiceUri());
		URI cimaxOgerServiceUri = URI.create(options.getCimaxOgerServiceUri());
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.OGER_DONE;
//		DocumentType targetDocumentType = options.getTargetDocumentType();

		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

//		/*
//		 * The pipeline will process each triple (URI, ProcessingStatusFlag,
//		 * DocumentType).
//		 */
//		LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- %s",
//				targetProcessingStatusFlag.name(), targetDocumentType.name(), ogerServiceUri));

		/*
		 * The OGER pipeline requires plain text documents, hence the type=TEXT and
		 * format=TEXT below. However, the selection of documents can be adjusted by
		 * specifying different pipeline keys and pipeline versions. These are set in
		 * the options for this pipeline.
		 */
		DocumentCriteria inputAugTextDocCriteria = new DocumentCriteria(DocumentType.AUGMENTED_TEXT,
				DocumentFormat.TEXT, options.getAugmentedTextPipelineKey(), options.getAugmentedTextPipelineVersion());
		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextPipelineKey(), options.getTextPipelineVersion());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(CollectionsUtil.createSet(inputTextDocCriteria, inputAugTextDocCriteria),
						options.getProject(), p, targetProcessingStatusFlag, requiredProcessStatusFlags,
						options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria csOutputDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_CS, DocumentFormat.BIONLP,
				PIPELINE_KEY, options.getOutputPipelineVersion());

		DocumentCriteria ciminOutputDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_CIMIN,
				DocumentFormat.BIONLP, PIPELINE_KEY, options.getOutputPipelineVersion());

		DocumentCriteria cimaxOutputDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_CIMAX,
				DocumentFormat.BIONLP, PIPELINE_KEY, options.getOutputPipelineVersion());

		// note: we are using the cs output doc criteria as the error doc criteria (any
		// of the three could be chosen)
		// note: enabling multithreaded service calls results in out of memory errors
		PCollectionTuple output = OgerFn.process(statusEntity2Content, csOgerServiceUri.toString(),
				ciminOgerServiceUri.toString(), cimaxOgerServiceUri.toString(), csOutputDocCriteria, timestamp,
				MultithreadedServiceCalls.DISABLED);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToCsAnnotation = output
				.get(OgerFn.CS_ANNOTATIONS_TAG);
		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToCiminAnnotation = output
				.get(OgerFn.CIMIN_ANNOTATIONS_TAG);
		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToCimaxAnnotation = output
				.get(OgerFn.CIMAX_ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(OgerFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToCsAnnotation.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		// output CS annotations
		PCollection<KV<String, List<String>>> nonredundantDocIdToCsAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToCsAnnotation);
		nonredundantDocIdToCsAnnotations
				.apply("cs annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(csOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("cs annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		// output CIMIN annotations
		PCollection<KV<String, List<String>>> nonredundantDocIdToCiminAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToCiminAnnotation);
		nonredundantDocIdToCiminAnnotations
				.apply("cimin annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(ciminOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("cimin annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		// output CIMAX annotations
		PCollection<KV<String, List<String>>> nonredundantDocIdToCimaxAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToCimaxAnnotation);
		nonredundantDocIdToCimaxAnnotations
				.apply("cimax annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(cimaxOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("cimax annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

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
