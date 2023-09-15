package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BulkIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.DocToBulk;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.ConstrainDocumentsToCollection;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Loads sentences into Elasticsearch for indexing. A flag specifies whether all
 * sentences should be loaded or just sentences that contain at least one
 * concept annotation.
 */
public class ElasticsearchLoadPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.ELASTICSEARCH_LOAD;
//	protected static final String DOCUMENT_ID_TO_CONCEPT_ID_FILE_PREFIX = "-to-concept";

	public enum SentenceInclusionFlag {
		INCLUDE_ALL_SENTENCES,
		/**
		 * Signifies that only sentences with overlapping concept annotations will be
		 * indexed in Elasticsearch
		 */
		INCLUDE_ONLY_SENTENCES_WITH_CONCEPTS
	}

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

//		@Description("The document type, e.g. CONCEPT_ALL, CONCEPT_MP, etc., indicating the document type containing the annotations that will be counted. This document type must be in the InputDocumentCriteria input parameter.")
//		DocumentType getDocTypeToCount();
//
//		void setDocTypeToCount(DocumentType value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("The document collection to process")
		@Required
		SentenceInclusionFlag getSentenceInclusionFlag();

		void setSentenceInclusionFlag(SentenceInclusionFlag value);

		@Description("The DocumentType from which to extract the concept annotations - CONCEPT_ALL or CONCEPT_ALL_UNFILTERED")
		@Required
		DocumentType getConceptDocumentType();

		void setConceptDocumentType(DocumentType value);

		@Description("Elasticsearch URLs - pipe-delimited String")
		@Required
		String getElasticsearchAddresses();

		void setElasticsearchAddresses(String indexName);

		@Description("Elasticsearch index name")
		@Required
		String getElasticsearchIndexName();

		void setElasticsearchIndexName(String indexName);

		@Description("Elasticsearch API key")
		@Required
		String getElasticsearchApiKey();

		void setElasticsearchApiKey(String apiKey);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);
		
		@Description("If yes, then the specified collection is used as a filter when searching for documents specified by the input doc criteria. If NO, then the collection filter is excluded. This is helpful when only the status entity has been assigned to a particular collection that we want to process. It may be inefficient in that more documents will be returned, and then filtered, but allows for processing of a collection assigned only the the status entities, e.g., the redo collections.")
		@Default.Enum("YES")
		ConstrainDocumentsToCollection getConstrainDocumentsToCollection();
		
		void setConstrainDocumentsToCollection(ConstrainDocumentsToCollection value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.ELASTICSEARCH_INDEX_DONE;
		Pipeline p = Pipeline.create(options);

		Set<ProcessingStatusFlag> requiredProcessStatusFlags = PipelineMain
				.compileRequiredProcessingStatusFlags(options.getRequiredProcessingStatusFlags());

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());

		// this is used just for failure messages
		DocumentCriteria errorCriteria = new DocumentCriteria(DocumentType.ELASTIC, DocumentFormat.JSON, PIPELINE_KEY,
				pipelineVersion);

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(), options.getConstrainDocumentsToCollection());

		PCollectionTuple output = ElasticsearchDocumentCreatorFn.createDocuments(statusEntity2Content, timestamp,
				inputDocCriteria, errorCriteria, options.getConceptDocumentType());

		/*
		 * store failures from sentence extraction
		 */
		PCollection<EtlFailureData> failures = output.get(ElasticsearchDocumentCreatorFn.ETL_FAILURE_TAG);
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ES document gen failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* esDocs are JSON string */
		PCollection<String> esDocs = output.get(ElasticsearchDocumentCreatorFn.ELASTICSEARCH_DOCUMENT_JSON_TAG);

		String[] esAddresses = options.getElasticsearchAddresses().split("\\|");

		ConnectionConfiguration connectionConfiguration = ElasticsearchIO.ConnectionConfiguration
				.create(esAddresses, options.getElasticsearchIndexName()).withApiKey(options.getElasticsearchApiKey());
		DocToBulk docToBulk = ElasticsearchIO.docToBulk().withConnectionConfiguration(connectionConfiguration);
		BulkIO bulkIO = ElasticsearchIO.bulkIO().withConnectionConfiguration(connectionConfiguration);
		esDocs.apply("index into ES", docToBulk).apply(bulkIO);

		p.run().waitUntilFinish();
	}

}
