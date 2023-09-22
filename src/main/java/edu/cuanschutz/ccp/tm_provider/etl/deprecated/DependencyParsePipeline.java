package edu.cuanschutz.ccp.tm_provider.etl.deprecated;

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

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.TurkuDepParserFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * NOTE: this is not in active use.
 * 
 * This Apache Beam pipeline processes documents with a dependency parser
 * reached via HTTP POST. Input is plain text; Output is CoNLL-U format.
 */
public class DependencyParsePipeline {

//	private final static Logger LOGGER = Logger.getLogger(DependencyParsePipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.DEPENDENCY_PARSE;

	public interface Options extends DataflowPipelineOptions {
		@Description("URI for the dependency parser service")
		String getDependencyParserServiceUri();

		void setDependencyParserServiceUri(String value);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getInputPipelineKey();

		void setInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getInputPipelineVersion();

		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);
	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		// we want to find documents that need dependency parsing
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.DP_DONE;
		// we require that the documents have a plain text version
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		/*
		 * The dependency parse pipeline requires plain text documents, hence the
		 * type=TEXT and format=TEXT below. However, the selection of documents can be
		 * adjusted by specifying different pipeline keys and pipeline versions. These
		 * are set in the options for this pipeline.
		 */
		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getInputPipelineKey(), options.getInputPipelineVersion());
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(CollectionsUtil.createSet(inputTextDocCriteria), options.getProject(), p,
						targetProcessingStatusFlag, requiredProcessStatusFlags, options.getCollection(),
						options.getOverwrite(), options.getOptionalDocumentSpecificCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = TurkuDepParserFn.process(statusEntity2Content,
				options.getDependencyParserServiceUri(), outputDocCriteria, timestamp);

		/*
		 * Processing of the plain text by the dependency parser results in 1) a
		 * PCollection mapping document ID to the CoNLL-U version of the document. 2) a
		 * PCollection logging any errors encountered during the dependency parse
		 * processing.
		 */

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToConllu = output.get(TurkuDepParserFn.CONLLU_TAG);
		PCollection<EtlFailureData> failures = output.get(TurkuDepParserFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToConllu.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the CoNLL-U document content in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantPlainText = PipelineMain
				.deduplicateDocuments(statusEntityToConllu);
		nonredundantPlainText
				.apply("conllu->document_entity",
						ParDo.of(new DocumentToEntityFn(outputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

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
