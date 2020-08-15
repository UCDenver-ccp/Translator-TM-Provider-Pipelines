package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.BigQueryExportFileBuilderFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentDownloadFn;
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
 * This Apache Beam pipeline processes documents with the OGER concept
 * recognition service reached via HTTP POST. Input is plain text; Output is
 * concept annotations in BioNLP format.
 */
public class BigQueryExportPipeline {

	private final static Logger LOGGER = Logger.getLogger(BigQueryExportPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.BIGQUERY_EXPORT;

	public interface Options extends DataflowPipelineOptions {
		@Description("Location of the output bucket")
		String getOutputBucket();

		void setOutputBucket(String value);

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
		LOGGER.log(Level.INFO, String.format("Running BigQuery export pipeline"));

		Pipeline p = Pipeline.create(options);
		PCollection<String> documentIds = getDocumentIdsToProcess(p, options.getCollection(), options.getOverwrite());
		List<DocumentCriteria> documentCriteria = populateDocumentCriteria(pipelineVersion);

		// returns map from document id to a map k=DocumentType, v=file contents
		PCollectionTuple docIdToAnnotationTuple = DocumentDownloadFn.process(documentIds, timestamp, documentCriteria);

		PCollection<KV<String, Map<DocumentType, String>>> docIdToAnnotations = docIdToAnnotationTuple
				.get(DocumentDownloadFn.OUTPUT_TAG);
		PCollection<EtlFailureData> annotationRetrievalFailures = docIdToAnnotationTuple
				.get(DocumentDownloadFn.FAILURE_TAG);

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = annotationRetrievalFailures
				.apply("annot_extract_failures->datastore", ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntities(failureEntities);
		nonredundantFailureEntities.apply("annot_extract_failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.BIGQUERY, DocumentFormat.BIGQUERY,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple bigqueryExports = BigQueryExportFileBuilderFn.processNoTrackDocIds(docIdToAnnotations,
				outputDocCriteria, timestamp);

		PCollection<String> docIdToBigQuery_annotationTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.ANNOTATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<String> docIdToBigQuery_inSectionTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.IN_SECTION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<String> docIdToBigQuery_inParagraphTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.IN_PARAGRAPH_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<String> docIdToBigQuery_inSentenceTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.IN_SENTENCE_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<String> docIdToBigQuery_inConceptTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.IN_CONCEPT_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<String> docIdToBigQuery_relationTable = bigqueryExports
				.get(BigQueryExportFileBuilderFn.RELATION_TABLE_OUTPUT_TAG_NO_TRACK_DOCID);
		PCollection<EtlFailureData> bigqueryExportFailures = bigqueryExports
				.get(BigQueryExportFileBuilderFn.FAILURE_TAG);

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		failureEntities = bigqueryExportFailures.apply("bq_export_failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		nonredundantFailureEntities = PipelineMain.deduplicateEntities(failureEntities);
		nonredundantFailureEntities.apply("bq_export_failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// write the output
		docIdToBigQuery_annotationTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/annotation.").withSuffix(".tsv"));
		docIdToBigQuery_inSectionTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/in-section.").withSuffix(".tsv"));
		docIdToBigQuery_inParagraphTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/in-paragraph.").withSuffix(".tsv"));
		docIdToBigQuery_inSentenceTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/in-sentence.").withSuffix(".tsv"));
		docIdToBigQuery_inConceptTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/in-concept.").withSuffix(".tsv"));
		docIdToBigQuery_relationTable.apply("write annotation table",
				TextIO.write().to(options.getOutputBucket() + "/relation.").withSuffix(".tsv"));

		PCollectionList<EtlFailureData> failureList = PCollectionList.of(annotationRetrievalFailures)
				.and(bigqueryExportFailures);
		PCollection<EtlFailureData> mergedFailures = failureList.apply(Flatten.<EtlFailureData>pCollections());

		// update the status for documents that were successfully processed
		PCollection<KV<String, String>> successStatus = DatastoreProcessingStatusUtil.getSuccessStatus(documentIds,
				mergedFailures, ProcessingStatusFlag.BIGQUERY_LOAD_FILE_EXPORT_DONE);
		List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
		statusList.add(successStatus);
		DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);

		p.run().waitUntilFinish();

	}

	private static List<DocumentCriteria> populateDocumentCriteria(String pipelineVersion) {
		List<DocumentCriteria> documentCriteria = Arrays.asList(
				new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT, pipelineVersion),
				new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP, PipelineKey.SENTENCE_SEGMENTATION,
						pipelineVersion),
				new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP, PipelineKey.BIOC_TO_TEXT,
						pipelineVersion),
				new DocumentCriteria(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU, PipelineKey.DEPENDENCY_PARSE,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_CL, DocumentFormat.BIONLP, PipelineKey.OGER, pipelineVersion),
//				new DocumentCriteria(DocumentType.CONCEPT_DOID, DocumentFormat.BIONLP, PipelineKey.OGER,
//						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_GO_BP, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_GO_CC, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_GO_MF, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
//				new DocumentCriteria(DocumentType.CONCEPT_HGNC, DocumentFormat.BIONLP, PipelineKey.OGER,
//						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_MOP, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_NCBITAXON, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_PR, DocumentFormat.BIONLP, PipelineKey.OGER, pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_SO, DocumentFormat.BIONLP, PipelineKey.OGER, pipelineVersion),
				new DocumentCriteria(DocumentType.CONCEPT_UBERON, DocumentFormat.BIONLP, PipelineKey.OGER,
						pipelineVersion));
		return documentCriteria;
	}

	private static PCollection<String> getDocumentIdsToProcess(Pipeline p, String collection,
			OverwriteOutput overwrite) {
		// we want to find documents that need BigQuery export
		ProcessingStatusFlag targetProcessStatusFlag = ProcessingStatusFlag.BIGQUERY_LOAD_FILE_EXPORT_DONE;
		// require that the documents be fully processed
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

//		,
//				ProcessingStatusFlag.DP_DONE);
//				,
//				
//				ProcessingStatusFlag.OGER_CHEBI_DONE, ProcessingStatusFlag.OGER_CL_DONE,
//				ProcessingStatusFlag.OGER_GO_BP_DONE, ProcessingStatusFlag.OGER_GO_CC_DONE);
//				ProcessingStatusFlag.OGER_GO_MF_DONE, ProcessingStatusFlag.OGER_MOP_DONE,
//				ProcessingStatusFlag.OGER_NCBITAXON_DONE, ProcessingStatusFlag.OGER_PR_DONE,
//				ProcessingStatusFlag.OGER_SO_DONE, ProcessingStatusFlag.OGER_UBERON_DONE);

		// query Cloud Datastore to find document IDs in need of processing
		DatastoreProcessingStatusUtil statusUtil = new DatastoreProcessingStatusUtil();
		List<String> documentIdsToProcess = statusUtil.getDocumentIdsInNeedOfProcessing(targetProcessStatusFlag,
				requiredProcessStatusFlags, collection, overwrite);

//		documentIdsToProcess = documentIdsToProcess.subList(0, 10);

		LOGGER.log(Level.INFO, String.format("Pipeline: %s, %d documents to process...", PIPELINE_KEY.name(),
				documentIdsToProcess.size()));

		PCollection<String> documentIds = p.apply(Create.of(documentIdsToProcess).withCoder(StringUtf8Coder.of()));
		return documentIds;
	}

}
