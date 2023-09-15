package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
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

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.FilterUnactionableTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * 
 * This class was originally designed to create a version of the plain txt of a
 * document that filters text we do not want to process, e.g., the reference
 * section. We refer to the filtered text as "unactionable".
 *
 */
public class FilterUnactionableTextPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.FILTER_UNACTIONABLE_TEXT;

	public interface Options extends DataflowPipelineOptions {
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getTextPipelineKey();

		void setTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getTextPipelineVersion();

		void setTextPipelineVersion(String value);

		@Description("This pipeline version will be used as part of the output document key/name")
		@Required
		String getOutputPipelineVersion();

		void setOutputPipelineVersion(String value);

		@Description("The name of the document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous imported documents")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("temporary - output the names of the top level sections")
		@Required
		String getOutputBucket();

		void setOutputBucket(String value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);
	}

	public static void main(String[] args) {
//		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.FILTER_UNACTIONABLE_DONE;
		Pipeline p = Pipeline.create(options);

		// require that documents have the original text, abbreviations, and segmented
		// sentences
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextPipelineKey(), options.getTextPipelineVersion());
		DocumentCriteria inputSectionDocCriteria = new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				options.getTextPipelineKey(), options.getTextPipelineVersion());

		Set<DocumentCriteria> requiredDocCriteria = CollectionsUtil.createSet(inputTextDocCriteria,
				inputSectionDocCriteria);
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(requiredDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.ACTIONABLE_TEXT, DocumentFormat.TEXT,
				PIPELINE_KEY, options.getOutputPipelineVersion());

		PCollectionTuple output = FilterUnactionableTextFn.process(statusEntity2Content, outputDocCriteria,
				requiredDocCriteria, timestamp, PIPELINE_KEY);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAugmentedText = output
				.get(FilterUnactionableTextFn.FILTERED_TEXT_TAG);
		PCollection<EtlFailureData> failures = output.get(FilterUnactionableTextFn.ETL_FAILURE_TAG);

		PCollection<String> topLevelSectionNames = output.get(FilterUnactionableTextFn.TOP_LEVEL_SECTION_TAG);

		topLevelSectionNames.apply("write tsv",
				TextIO.write().to(options.getOutputBucket()).withSuffix("." + options.getCollection() + ".tsv"));

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToAugmentedText.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
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
				.deduplicateDocuments(statusEntityToAugmentedText);
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
