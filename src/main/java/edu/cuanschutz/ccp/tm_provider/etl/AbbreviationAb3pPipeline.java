
package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.examples.subprocess.SubProcessPipelineOptions;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
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
import org.apache.beam.sdk.values.TupleTagList;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.AbbreviationFn;
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
 * 
 * In order for this pipeline to work, the compiled binary for the Ab3P system
 * and all resource files must be present in a Google Bucket. These files will
 * be copied to the worker during setup. This strategy for running external
 * system is based on
 * https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/subprocess/ExampleEchoPipeline.java
 * 
 * Required Ab3P files:
 * <ul>
 * <li>identify_abbr
 * <li>path_Ab3P
 * <li>WordData/*
 * </ul>
 * 
 */
public class AbbreviationAb3pPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.ABBREVIATION;

	public interface Options extends SubProcessPipelineOptions, DataflowPipelineOptions {

		@Description("The name of the compiled abbreviation detection binary")
		@Required
		String getAbbreviationBinaryName();

		void setAbbreviationBinaryName(String value);

		@Description("A pipe-delimited list of all files that need to be downloaded to the worker in order for the compiled binary to function properly")
		@Required
		String getBinaryFileAndDependencies();

		void setBinaryFileAndDependencies(String value);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getInputSentencePipelineKey();

		void setInputSentencePipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getInputSentencePipelineVersion();

		void setInputSentencePipelineVersion(String value);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getInputTextPipelineKey();

		void setInputTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getInputTextPipelineVersion();

		void setInputTextPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.ABBREVIATIONS_DONE;

		Pipeline p = Pipeline.create(options);

		// Setup the Configuration option used with all transforms
		SubProcessConfiguration configuration = options.getSubProcessConfiguration();
		String abbreviationsBinaryName = options.getAbbreviationBinaryName();

		/*
		 * require that the documents have a plain text version and that they have been
		 * segmented into sentences
		 */
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.SENTENCE_DONE);

		/*
		 * The Abbreviation pipeline requires sentences and the document text.
		 */
		Set<DocumentCriteria> inputDocCriteria = CollectionsUtil.createSet(
				new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
						options.getInputSentencePipelineKey(), options.getInputSentencePipelineVersion()),
				new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, options.getInputTextPipelineKey(),
						options.getInputTextPipelineVersion()));
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.ABBREVIATIONS, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);

		List<String> filesToDownloadToWorker = Arrays.asList(options.getBinaryFileAndDependencies().split("\\|"));
		AbbreviationFn abbreviationFn = new AbbreviationFn(configuration, abbreviationsBinaryName,
				filesToDownloadToWorker, outputDocCriteria, timestamp);

		ParDo.MultiOutput<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>> parDo;

		parDo = ParDo.of(abbreviationFn).withOutputTags(AbbreviationFn.ABBREVIATIONS_TAG,
				TupleTagList.of(AbbreviationFn.ETL_FAILURE_TAG));

		PCollectionTuple output = statusEntity2Content.apply("Identify abbreviations in sentences", parDo);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAnnotation = output
				.get(AbbreviationFn.ABBREVIATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(AbbreviationFn.ETL_FAILURE_TAG);

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