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
import edu.cuanschutz.ccp.tm_provider.etl.fn.OpenNLPSentenceSegmentFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * This pipeline takes as input the CoNNL-U Dependency Parse output of each
 * document and the document text. It uses the dependency parses to divide the
 * document into sentences. A Bionlp formatted sentence file is generated and
 * saved to Datastore for each document.
 */
public class DependencyParseToSentencePipeline2 {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.DEPENDENCY_PARSE_TO_SENTENCE;

	public interface Options extends DataflowPipelineOptions {
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getTextInputPipelineKey();

		void setTextInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getTextInputPipelineVersion();

		void setTextInputPipelineVersion(String value);

		@Description("The version of the dependency parse documents")
		String getDpPipelineVersion();

		void setDpPipelineVersion(String value);

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

		Pipeline p = Pipeline.create(options);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.SENTENCE_DONE;
		// we require that the documents have a plain text version and dependency parse
		// output
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.DP_DONE);

		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextInputPipelineKey(), options.getTextInputPipelineVersion());
//		DocumentCriteria inputDpDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.,
//				options.getTextInputPipelineKey(), options.getTextInputPipelineVersion());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(CollectionsUtil.createSet(inputTextDocCriteria), options.getProject(), p,
						targetProcessingStatusFlag, requiredProcessStatusFlags, options.getCollection(),
						options.getOverwrite());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);

		/* initialize to load the sentence segmentation model */
		PCollectionTuple output = OpenNLPSentenceSegmentFn.process(statusEntity2Content, outputDocCriteria, timestamp);

		/*
		 * Processing of the plain text by the OpenNLP sentence segmenter 1) a
		 * PCollection mapping document ID to the BioNLP version of the sentence
		 * annotations. 2) a PCollection logging any errors encountered during sentence
		 * segmentation processing.
		 */

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToSentenceBioNLP = output
				.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG);
		PCollection<EtlFailureData> failures = output.get(OpenNLPSentenceSegmentFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToSentenceBioNLP.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
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
				.deduplicateDocuments(statusEntityToSentenceBioNLP);
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
