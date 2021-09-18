package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.HashSet;
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

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.AddSuperClassAnnots;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
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
public class ConceptCooccurrenceCountsPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.CONCEPT_COOCCURRENCE_COUNTS;
	protected static final String CONCEPT_ID_TO_DOC_FILE_PREFIX = "concept-to-";
	protected static final String CONCEPT_PAIR_TO_DOC_FILE_PREFIX = "concept-pair-to-";
	protected static final String LEVEL_TO_CONCEPT_COUNT_FILE_PREFIX = "-to-concept-count";

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

		@Description("If YES, then when counting concepts, all superclasses for a given concept are also added and counted.")
		AddSuperClassAnnots getAddSuperClassAnnots();

		void setAddSuperClassAnnots(AddSuperClassAnnots value);

		@Description("The document type, e.g. CONCEPT_ALL, CONCEPT_MP, etc., indicating the document type containing the annotations that will be counted. This document type must be in the InputDocumentCriteria input parameter.")
		DocumentType getDocTypeToCount();

		void setDocTypeToCount(DocumentType value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("'levels' of cooccurrence for which to compute counts, e.g. Document, Sentence, etc. This paramter should be pipe-delimited values from the CooccurLevel enum.")
		String getCooccurLevels();

		void setCooccurLevels(String value);

		@Description("If SIMPLE, then this will produce only a subset of all possible counts. If FULL, then all counts are produced")
		CountType getCountType();

		void setCountType(CountType value);

	}

	public enum CountType {
		/**
		 * A select of "FULL" will compute all concept counts needed for various
		 * cooccurrence metrics
		 */
		FULL,
		/**
		 * A selection of "SIMPLE" will result in only the concept id -to- document id
		 * data being serialized.
		 */
		SIMPLE
	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.NORMALIZED_GOOGLE_DISTANCE_STORE_COUNTS_DONE;
		Pipeline p = Pipeline.create(options);

		Set<CooccurLevel> cooccurLevels = new HashSet<CooccurLevel>();
		for (String level : options.getCooccurLevels().split("\\|")) {
			cooccurLevels.add(CooccurLevel.valueOf(level));
		}

		Set<ProcessingStatusFlag> requiredProcessStatusFlags = PipelineMain
				.compileRequiredProcessingStatusFlags(options.getRequiredProcessingStatusFlags());

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		final PCollectionView<Map<String, Set<String>>> ancestorMapView = PCollectionUtil.fromKeyToSetTwoColumnFiles(
				"ancestor map", p, options.getAncestorMapFilePath(), options.getAncestorMapFileDelimiter(),
				options.getAncestorMapFileSetDelimiter(), Compression.GZIP).apply(View.<String, Set<String>>asMap());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.NGD_COUNTS, DocumentFormat.TSV,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = ConceptCooccurrenceCountsFn.computeCounts(statusEntity2Content, outputDocCriteria,
				timestamp, inputDocCriteria, cooccurLevels, options.getAddSuperClassAnnots(),
				options.getDocTypeToCount(), options.getCountType(), ancestorMapView);

		/*
		 * store failures from sentence extraction
		 */
		PCollection<EtlFailureData> failures = output.get(ConceptCooccurrenceCountsFn.ETL_FAILURE_TAG);
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ngd count failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* ==== Store counts below ==== */

		if (cooccurLevels.contains(CooccurLevel.DOCUMENT)) {
			PCollection<String> conceptIdToDocId = output.get(ConceptCooccurrenceCountsFn.SINGLETON_TO_DOC_ID);
			conceptIdToDocId.apply("write concept-id to doc-id",
					TextIO.write()
							.to(options.getOutputBucket() + "/"
									+ getConceptIdToLevelFileNamePrefix(CooccurLevel.DOCUMENT, options.getCollection()))
							.withSuffix(".tsv"));
		}

		if (cooccurLevels.contains(CooccurLevel.TITLE)) {
			PCollection<String> conceptIdToTitleId = output.get(ConceptCooccurrenceCountsFn.SINGLETON_TO_TITLE_ID);
			conceptIdToTitleId.apply("write concept-id to title-id",
					TextIO.write()
							.to(options.getOutputBucket() + "/"
									+ getConceptIdToLevelFileNamePrefix(CooccurLevel.TITLE, options.getCollection()))
							.withSuffix(".tsv"));
		}

		if (cooccurLevels.contains(CooccurLevel.SENTENCE)) {
			PCollection<String> conceptIdToSentenceId = output
					.get(ConceptCooccurrenceCountsFn.SINGLETON_TO_SENTENCE_ID);
			conceptIdToSentenceId.apply("write concept-id to sentence-id",
					TextIO.write()
							.to(options.getOutputBucket() + "/"
									+ getConceptIdToLevelFileNamePrefix(CooccurLevel.SENTENCE, options.getCollection()))
							.withSuffix(".tsv"));
		}

		if (cooccurLevels.contains(CooccurLevel.ABSTRACT)) {
			PCollection<String> conceptIdToAbstractId = output
					.get(ConceptCooccurrenceCountsFn.SINGLETON_TO_ABSTRACT_ID);
			conceptIdToAbstractId.apply("write concept-id to abstract-id",
					TextIO.write()
							.to(options.getOutputBucket() + "/"
									+ getConceptIdToLevelFileNamePrefix(CooccurLevel.ABSTRACT, options.getCollection()))
							.withSuffix(".tsv"));
		}

		if (options.getCountType() == CountType.FULL) {

			if (cooccurLevels.contains(CooccurLevel.DOCUMENT)) {
				PCollection<String> conceptPairIdToDocId = output.get(ConceptCooccurrenceCountsFn.PAIR_TO_DOC_ID);
				PCollection<String> docIdToConceptCount = output
						.get(ConceptCooccurrenceCountsFn.DOC_ID_TO_CONCEPT_COUNT);

				conceptPairIdToDocId.apply("write concept pair to doc-id",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getConceptPairToLevelFileNamePrefix(CooccurLevel.DOCUMENT, options.getCollection()))
								.withSuffix(".tsv"));

				docIdToConceptCount.apply("write doc-id to concept count",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getLevelToConceptCountFileNamePrefix(CooccurLevel.DOCUMENT, options.getCollection()))
								.withSuffix(".tsv"));
			}

			if (cooccurLevels.contains(CooccurLevel.TITLE)) {
				PCollection<String> conceptPairIdToTitleId = output.get(ConceptCooccurrenceCountsFn.PAIR_TO_TITLE_ID);
				PCollection<String> titleIdToConceptCount = output
						.get(ConceptCooccurrenceCountsFn.TITLE_ID_TO_CONCEPT_COUNT);
				conceptPairIdToTitleId.apply("write concept pair to title-id",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getConceptPairToLevelFileNamePrefix(CooccurLevel.TITLE, options.getCollection()))
								.withSuffix(".tsv"));

				titleIdToConceptCount.apply("write title-id to concept count",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getLevelToConceptCountFileNamePrefix(CooccurLevel.TITLE, options.getCollection()))
								.withSuffix(".tsv"));
			}

			if (cooccurLevels.contains(CooccurLevel.SENTENCE)) {
				PCollection<String> conceptPairIdToSentenceId = output
						.get(ConceptCooccurrenceCountsFn.PAIR_TO_SENTENCE_ID);
				PCollection<String> sentenceIdToConceptCount = output
						.get(ConceptCooccurrenceCountsFn.SENTENCE_ID_TO_CONCEPT_COUNT);
				conceptPairIdToSentenceId.apply("write concept pair to sentence-id",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getConceptPairToLevelFileNamePrefix(CooccurLevel.SENTENCE, options.getCollection()))
								.withSuffix(".tsv"));

				sentenceIdToConceptCount.apply("write sentence-id to concept count",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getLevelToConceptCountFileNamePrefix(CooccurLevel.SENTENCE, options.getCollection()))
								.withSuffix(".tsv"));
			}

			if (cooccurLevels.contains(CooccurLevel.ABSTRACT)) {
				PCollection<String> conceptPairIdToAbstractId = output
						.get(ConceptCooccurrenceCountsFn.PAIR_TO_ABSTRACT_ID);
				PCollection<String> abstractIdToConceptCount = output
						.get(ConceptCooccurrenceCountsFn.ABSTRACT_ID_TO_CONCEPT_COUNT);
				conceptPairIdToAbstractId.apply("write concept pair to abstract-id",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getConceptPairToLevelFileNamePrefix(CooccurLevel.ABSTRACT, options.getCollection()))
								.withSuffix(".tsv"));

				abstractIdToConceptCount.apply("write abstract-id to concept count",
						TextIO.write().to(options.getOutputBucket() + "/"
								+ getLevelToConceptCountFileNamePrefix(CooccurLevel.ABSTRACT, options.getCollection()))
								.withSuffix(".tsv"));
			}
		}

		p.run().waitUntilFinish();
	}

	protected static String getLevelToConceptCountFileNamePrefix(CooccurLevel level, String collection) {
		return getLevelToConceptCountFileNamePrefix(level) + collection;
	}

	public static String getLevelToConceptCountFileNamePrefix(CooccurLevel level) {
		return String.format("%s%s.", level.name().toLowerCase(), LEVEL_TO_CONCEPT_COUNT_FILE_PREFIX);
	}

	protected static String getConceptPairToLevelFileNamePrefix(CooccurLevel level, String collection) {
		return getConceptPairToLevelFileNamePrefix(level) + collection;
	}

	public static String getConceptPairToLevelFileNamePrefix(CooccurLevel level) {
		return String.format("%s%s.", CONCEPT_PAIR_TO_DOC_FILE_PREFIX, level.name().toLowerCase());
	}

	protected static String getConceptIdToLevelFileNamePrefix(CooccurLevel level, String collection) {
		return getConceptIdToLevelFileNamePrefix(level) + collection;
	}

	public static String getConceptIdToLevelFileNamePrefix(CooccurLevel level) {
		return String.format("%s%s.", CONCEPT_ID_TO_DOC_FILE_PREFIX, level.name().toLowerCase());
	}

}
