package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
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

import edu.cuanschutz.ccp.tm_provider.etl.fn.DependencyParseImportFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Consumes Dependency Parse CONLL-U files that contain multiple documents,
 * parses them into individual documents, then stores the CONLL-U in Cloud
 * Datastore
 */
public class DependencyParseStoragePipeline {

	private static final String DEPENDENY_PARSES_CONLLU_FILE_SUFFIX = ".dependency_parses.conllu.gz";
	private static final PipelineKey PIPELINE_KEY = PipelineKey.DEPENDENCY_PARSE_IMPORT;

	public interface Options extends DataflowPipelineOptions {
		@Description("GCS path to the CONLL-U files to load - this is the base path with the next directory expected to be the collection name.")
		@Required
		String getBaseDependencyParseFilePath();

		void setBaseDependencyParseFilePath(String path);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous CONLL-U data")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);
	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		String dependencyParseFilePattern = options.getBaseDependencyParseFilePath() + "/" + options.getCollection()
				+ "/*" + DEPENDENY_PARSES_CONLLU_FILE_SUFFIX;
		PCollection<ReadableFile> files = p
				.apply("get CONLL-U files to load", FileIO.match().filepattern(dependencyParseFilePattern))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));

		DocumentCriteria conlluDocCriteria = new DocumentCriteria(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = DependencyParseImportFn.process(files, conlluDocCriteria, timestamp);

		PCollection<KV<String, List<String>>> docIdToDepParseConllu = output.get(DependencyParseImportFn.CONLLU_TAG);
		PCollection<EtlFailureData> failures = output.get(DependencyParseImportFn.ETL_FAILURE_TAG);

		/*
		 * In this case, we have included the collections as metadata in the dependency
		 * parse input, and hence output, so that we can grab it here. It is used so
		 * that all relevant collections are applied to the dependency parse documents
		 * when they are stored in Datastore
		 */
		PCollectionView<Map<String, Set<String>>> documentIdToCollections = output
				.get(DependencyParseImportFn.DOC_ID_TO_COLLECTIONS_TAG).apply(View.<String, Set<String>>asMap());

		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantDepParseConllu = PipelineMain
				.deduplicateDocumentsByStringKey(docIdToDepParseConllu);
		nonredundantDepParseConllu
				.apply("dp_conllu->dp_entity",
						ParDo.of(new DocumentToEntityFn(conlluDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("dp_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * Below - update status entities in bulk for the new dependency parse documents
		 */

		/*
		 * This PCollection<KV<String, String>> maps from document IDs to
		 * targetProcessingStatus flags that should be set to true in the bulk update
		 */
		PCollection<KV<String, String>> dpSuccessStatus = DatastoreProcessingStatusUtil.getSuccessStatus(
				nonredundantDepParseConllu.apply(Keys.<String>create()), failures, ProcessingStatusFlag.DP_DONE);

		List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
		statusList.add(dpSuccessStatus);
		DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);

		p.run().waitUntilFinish();
	}

}
