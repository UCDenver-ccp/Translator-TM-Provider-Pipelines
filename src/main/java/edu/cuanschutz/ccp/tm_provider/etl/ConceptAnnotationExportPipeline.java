package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptAnnotationExportFileBuilderFn;
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
 * This Apache Beam pipeline exports the post-processed annotations in BioNLP
 * format and the document text into files in a bucket.
 */
public class ConceptAnnotationExportPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.CONCEPT_ANNOTATION_EXPORT;

	public interface Options extends DataflowPipelineOptions {
		@Description("Location of the output bucket")
		@Required
		String getOutputBucket();

		void setOutputBucket(String value);

		@Description("This pipeline key will be used to select the input text documents that will be exported")
		@Required
		PipelineKey getTextInputPipelineKey();

		void setTextInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be exported")
		@Required
		String getTextInputPipelineVersion();

		void setTextInputPipelineVersion(String value);

		@Description("This pipeline version will be used to select the concept annotation documents that will be exported")
		@Required
		String getConceptPostProcessPipelineVersion();

		void setConceptPostProcessPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		/*
		 * The target processing status flag is set to null here because we won't be
		 * updating the status documents with the status of the export. There is no need
		 * to update as we are not adding content to Datastore, we are simply writing
		 * files to a bucket.
		 */
		ProcessingStatusFlag targetProcessingStatusFlag = null;

		Pipeline p = Pipeline.create(options);

		/*
		 * require that the documents have a plain text version and have post-processed
		 * concept annotations
		 */
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE);

		DocumentCriteria textDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextInputPipelineKey(), options.getTextInputPipelineVersion());
		DocumentCriteria conceptDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_ALL, DocumentFormat.BIONLP,
				PipelineKey.CONCEPT_POST_PROCESS, options.getConceptPostProcessPipelineVersion());
		Set<DocumentCriteria> inputDocumentCriteria = CollectionsUtil.createSet(textDocCriteria, conceptDocCriteria);

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocumentCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), OverwriteOutput.YES,
						options.getOptionalDocumentSpecificCollection());

		/*
		 * outputDocCriteria is only used to create a failure document if an error
		 * occurs
		 */
		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PIPELINE_KEY,
				pipelineVersion);

		PCollectionTuple output = ConceptAnnotationExportFileBuilderFn.process(statusEntity2Content, outputDocCriteria,
				timestamp);

		PCollection<EtlFailureData> failures = output.get(ConceptAnnotationExportFileBuilderFn.FAILURE_TAG);
		PCollection<String> outputDocumentText = output.get(ConceptAnnotationExportFileBuilderFn.TEXT_OUTPUT_TAG);
		PCollection<String> outputConceptAnnotationsInBionlpFormat = output
				.get(ConceptAnnotationExportFileBuilderFn.ANNOTATION_IN_BIONLP_OUTPUT_TAG);

		outputDocumentText.apply("write document text",
				TextIO.write().to(options.getOutputBucket()).withSuffix("." + options.getCollection() + ".text"));

		outputConceptAnnotationsInBionlpFormat.apply("write annotation bionlp",
				TextIO.write().to(options.getOutputBucket()).withSuffix("." + options.getCollection() + ".bionlp"));

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
