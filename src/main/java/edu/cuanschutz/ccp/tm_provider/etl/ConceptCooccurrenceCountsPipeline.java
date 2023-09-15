package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.HashSet;
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

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
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
	protected static final String DOCUMENT_ID_TO_CONCEPT_ID_FILE_PREFIX = "-to-concept";

	public interface Options extends DataflowPipelineOptions {
		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0")
		@Required
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("pipe-delimited list of processing status flags that will be used to query for status entities from Datastore")
		@Required
		String getRequiredProcessingStatusFlags();

		void setRequiredProcessingStatusFlags(String flags);

		@Description("The document type, e.g. CONCEPT_ALL, CONCEPT_MP, etc., indicating the document type containing the annotations that will be counted. This document type must be in the InputDocumentCriteria input parameter.")
		@Required
		DocumentType getDocTypeToCount();

		void setDocTypeToCount(DocumentType value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		@Required
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("'levels' of cooccurrence for which to compute counts, e.g. Document, Sentence, etc. This paramter should be pipe-delimited values from the CooccurLevel enum.")
		@Required
		String getCooccurLevels();

		void setCooccurLevels(String value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.CONCEPT_COOCCURRENCE_COUNTS_DONE;
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
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.NGD_COUNTS, DocumentFormat.TSV,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = ConceptCooccurrenceCountsFn.computeCounts(statusEntity2Content, outputDocCriteria,
				timestamp, inputDocCriteria, cooccurLevels, options.getDocTypeToCount());

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
			serializeDocIdToConceptIds(options.getOutputBucket(), options.getCollection(), output,
					CooccurLevel.DOCUMENT);
		}

		if (cooccurLevels.contains(CooccurLevel.TITLE)) {
			serializeDocIdToConceptIds(options.getOutputBucket(), options.getCollection(), output, CooccurLevel.TITLE);
		}

		if (cooccurLevels.contains(CooccurLevel.SENTENCE)) {
			serializeDocIdToConceptIds(options.getOutputBucket(), options.getCollection(), output,
					CooccurLevel.SENTENCE);
		}

		if (cooccurLevels.contains(CooccurLevel.ABSTRACT)) {
			serializeDocIdToConceptIds(options.getOutputBucket(), options.getCollection(), output,
					CooccurLevel.ABSTRACT);
		}

		p.run().waitUntilFinish();
	}

	/**
	 * Given a CooccurLevel, extract the relevant PCollection from the output and
	 * serialize the strings to file. These strings should be of the format:
	 * DOCUMENT_ID [tab] CONCEPT1|CONCEPT2|...
	 * 
	 * @param options
	 * @param output
	 * @param level
	 */
	private static void serializeDocIdToConceptIds(String outputBucket, String collection, PCollectionTuple output,
			CooccurLevel level) {
		PCollection<String> conceptIdToDocId = output.get(level.getOutputTag());
		conceptIdToDocId.apply("docid-to-conceptid - " + level.name().toLowerCase(),
				TextIO.write().to(outputBucket + "/" + getDocumentIdToConceptIdsFileNamePrefix(level, collection))
						.withSuffix(".tsv"));
	}

	protected static String getDocumentIdToConceptIdsFileNamePrefix(CooccurLevel level, String collection) {
		return getDocumentIdToConceptIdsFileNamePrefix(level) + collection;
	}

	public static String getDocumentIdToConceptIdsFileNamePrefix(CooccurLevel level) {
		return String.format("%s%s.", level.name().toLowerCase(), DOCUMENT_ID_TO_CONCEPT_ID_FILE_PREFIX);
	}

}
