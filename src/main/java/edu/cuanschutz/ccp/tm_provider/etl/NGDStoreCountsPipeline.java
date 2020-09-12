package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.NormalizedGoogleDistanceFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.NormalizedGoogleDistanceFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Computes normalized google distance between concepts
 */
public class NGDStoreCountsPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.NORMALIZED_GOOGLE_DISTANCE_CONCEPT_STORE_COUNTS;

	public interface Options extends DataflowPipelineOptions {
		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0")
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("pipe-delimited list of processing status flags that will be used to query for status entities from Datastore")
		String getRequiredProcessingStatusFlags();

		void setRequiredProcessingStatusFlags(String flags);

		@Description("path to (pattern for) the file(s) containing mappings from ontology class to ancestor classes")
		String getAncestorMapFilePath();

		void setAncestorMapFilePath(String path);

		@Description("delimiter used to separate columns in the ancestor map file")
		Delimiter getAncestorMapFileDelimiter();

		void setAncestorMapFileDelimiter(Delimiter delimiter);

		@Description("delimiter used to separate items in the set in the second column of the ancestor map file")
		Delimiter getAncestorMapFileSetDelimiter();

		void setAncestorMapFileSetDelimiter(Delimiter delimiter);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("'level' of cooccurrence, e.g. Document, Sentence, etc.")
		CooccurLevel getCooccurLevel();

		void setCooccurLevel(CooccurLevel value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.NORMALIZED_GOOGLE_DISTANCE_STORE_COUNTS_DONE;
		Pipeline p = Pipeline.create(options);

		Set<ProcessingStatusFlag> requiredProcessStatusFlags = PipelineMain
				.compileRequiredProcessingStatusFlags(options.getRequiredProcessingStatusFlags());

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		final PCollectionView<Map<String, Set<String>>> ancestorMapView = PCollectionUtil
				.fromKeyToSetTwoColumnFiles(p, options.getAncestorMapFilePath(), options.getAncestorMapFileDelimiter(),
						options.getAncestorMapFileSetDelimiter(), Compression.GZIP)
				.apply(View.<String, Set<String>>asMap());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.NGD_COUNTS, DocumentFormat.TSV,
				PIPELINE_KEY, pipelineVersion);

		// TODO NOTE: where is the ancestor map coming from?? load from files and
		// convert to map view -- use as side input
		PCollectionTuple output = NormalizedGoogleDistanceFn.computeCounts(statusEntity2Content, outputDocCriteria,
				timestamp, inputDocCriteria, options.getCooccurLevel(), ancestorMapView);

		PCollection<String> conceptIdToDocId = output.get(NormalizedGoogleDistanceFn.SINGLETON_TO_DOCID);
		PCollection<String> conceptPairIdToDocId = output.get(NormalizedGoogleDistanceFn.PAIR_TO_DOCID);
		PCollection<String> docIdToConceptCount = output.get(NormalizedGoogleDistanceFn.DOCID_TO_CONCEPT_COUNT);
		PCollection<EtlFailureData> failures = output.get(NormalizedGoogleDistanceFn.ETL_FAILURE_TAG);

		/*
		 * store failures from sentence extraction
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ngd count failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// TODO NOTE: only write doc-id if it is the doc-id - could be sentence id

		// output count files
		conceptIdToDocId.apply("write concept-id to doc-id file",
				TextIO.write().to(options.getOutputBucket()
						+ String.format("/concept-to-doc.%s.%s", options.getCollection(), options.getCooccurLevel()))
						.withSuffix(".nodes.tsv"));

		conceptPairIdToDocId.apply("write concept pair to doc-id file", TextIO.write().to(options.getOutputBucket()
				+ String.format("/concept-pair-to-doc.%s.%s", options.getCollection(), options.getCooccurLevel()))
				.withSuffix(".nodes.tsv"));

		docIdToConceptCount.apply("write doc-id to concept count file", TextIO.write().to(options.getOutputBucket()
				+ String.format("/doc-to-concept-count.%s.%s", options.getCollection(), options.getCooccurLevel()))
				.withSuffix(".nodes.tsv"));

		p.run().waitUntilFinish();
	}

}
