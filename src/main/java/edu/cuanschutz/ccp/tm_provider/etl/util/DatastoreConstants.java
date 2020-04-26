package edu.cuanschutz.ccp.tm_provider.etl.util;

public interface DatastoreConstants {

	/**
	 * There is a limit to the size of Strings stored in Cloud DataStore. The number
	 * below is slightly below the limit and is used to make sure that Strings
	 * aren't too big to be stored.
	 */
	public static final int MAX_STRING_STORAGE_SIZE_IN_BYTES = 1048000;

	/**
	 * The name of the pipeline used to produce the document
	 */
	public static final String PIPELINE_KEY = "pipeline";
	/**
	 * The version of the pipeline used to produce the document
	 */
	public static final String PIPELINE_VERSION = "pipeline-version";

	///////////////////////////////////////////////////////////////////////////
	///////////////////// DOCUMENT ENTITY PROPERTIES //////////////////////////
	///////////////////////////////////////////////////////////////////////////

	/**
	 * The Cloud Datastore entity type used for storing documents
	 */
	public static final String DOCUMENT_KIND = "document";
	/**
	 * The document id property for the document entity type, e.g. PMC12345
	 */
	public static final String DOCUMENT_PROPERTY_ID = "id";
	/**
	 * 
	 */
	public static final String DOCUMENT_PROPERTY_CHUNK_ID = "chunk_id";
	/**
	 * 
	 */
	public static final String DOCUMENT_PROPERTY_CHUNK_TOTAL = "chunk_total";
	/**
	 * This property stores the content of the document, e.g. XML for a BioC XML
	 * document
	 */
	public static final String DOCUMENT_PROPERTY_CONTENT = "content";
	/**
	 * This property indicates the format of the stored content. See
	 * {@link DocumentFormat}.
	 */
	public static final String DOCUMENT_PROPERTY_FORMAT = "format";
	/**
	 * This property indicates the type of the stored content. See
	 * {@link DocumentType}
	 */
	public static final String DOCUMENT_PROPERTY_TYPE = "type";
	/**
	 * The pipeline used to produce the document
	 */
	public static final String DOCUMENT_PROPERTY_PIPELINE = PIPELINE_KEY;
	/**
	 * The version of the pipeline used to produce the document
	 */
	public static final String DOCUMENT_PROPERTY_PIPELINE_VERSION = PIPELINE_VERSION;

	///////////////////////////////////////////////////////////////////////////
	////////////////////// FAILURE ENTITY PROPERTIES //////////////////////////
	///////////////////////////////////////////////////////////////////////////

	/**
	 * The Cloud Datastore entity type used for logging processing failures
	 */
	public static final String FAILURE_KIND = "failure";
	/**
	 * The document ID associated with the failure
	 */
	public static final String FAILURE_PROPERTY_DOCUMENT_ID = "document_id";
	/**
	 * The document type associated with the failure
	 */
	public static final String FAILURE_PROPERTY_DOCUMENT_TYPE = "document_type";
	/**
	 * 
	 */
	public static final String FAILURE_PROPERTY_DOCUMENT_FORMAT = "document_format";
	/**
	 * The message provided by the exception that was indicated for the failure
	 */
	public static final String FAILURE_PROPERTY_MESSAGE = "message";
	/**
	 * The stacktrace of the exception that was indicated for the failure
	 */
	public static final String FAILURE_PROPERTY_STACKTRACE = "stacktrace";
	/**
	 * The pipeline where the failure occurred
	 */
	public static final String FAILURE_PROPERTY_PIPELINE = PIPELINE_KEY;
	/**
	 * The version of the pipeline when the failure occurred
	 */
	public static final String FAILURE_PROPERTY_PIPELINE_VERSION = PIPELINE_VERSION;
	/**
	 * The time when the failure occurred
	 */
	public static final String FAILURE_PROPERTY_TIMESTAMP = "timestamp";

	///////////////////////////////////////////////////////////////////////////
	/////////////////// PROCESSING STATUS ENTITY PROPERTIES ///////////////////
	///////////////////////////////////////////////////////////////////////////

	/**
	 * The Cloud Datastore entity type used for logging processing status for each
	 * document
	 */
	public static final String STATUS_KIND = "status";
	/**
	 * The document ID corresponding to this status data
	 */
	public static final String STATUS_PROPERTY_DOCUMENT_ID = "document_id";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_TEXT_DONE = "text";
	public static final String STATUS_PROPERTY_SECTIONS_DONE = "sections";
	/**
	 * true if dependency parsing is complete
	 */
	public static final String STATUS_PROPERTY_DEPENDENCY_PARSE_DONE = "dep";
	
	
	public static final String STATUS_PROPERTY_SENTENCE_SEGMENTATION_DONE = "sent";

	////////////////////////////////////////////////////////////////
	/////////////////// public static final String STATUS_PROPERTY_OGER CONCEPT
	//////////////////////////////////////////////////////////////// PROCESSING
	//////////////////////////////////////////////////////////////// ////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_CHEBI_DONE = "oger_chebi";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_CL_DONE = "oger_cl";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_GO_BP_DONE = "oger_gobp";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_GO_CC_DONE = "oger_gocc";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_GO_MF_DONE = "oger_gomf";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_MOP_DONE = "oger_mop";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_NCBITAXON_DONE = "oger_ncbitaxon";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_SO_DONE = "oger_so";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_PR_DONE = "oger_pr";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_OGER_UBERON_DONE = "oger_uberon";

	////////////////////////////////////////////////////////////////
	////////////////// BERT CONCEPT PROCESSING /////////////////////
	////////////////////////////////////////////////////////////////

//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_CHEBI_IDS_DONE = "bert_chebi_ids";
//	public static final String STATUS_PROPERTY_BERT_CHEBI_SPANS_DONE = "bert_chebi_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_CL_IDS_DONE = "bert_cl_ids";
//	public static final String STATUS_PROPERTY_BERT_CL_SPANS_DONE = "bert_cl_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_GO_BP_IDS_DONE = "bert_gobp_ids";
//	public static final String STATUS_PROPERTY_BERT_GO_BP_SPANS_DONE = "bert_gobp_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_GO_CC_IDS_DONE = "bert_gocc_ids";
//	public static final String STATUS_PROPERTY_BERT_GO_CC_SPANS_DONE = "bert_gocc_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_GO_MF_IDS_DONE = "bert_gomf_ids";
//	public static final String STATUS_PROPERTY_BERT_GO_MF_SPANS_DONE = "bert_gomf_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_MOP_IDS_DONE = "bert_mop_ids";
//	public static final String STATUS_PROPERTY_BERT_MOP_SPANS_DONE = "bert_mop_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_NCBITAXON_IDS_DONE = "bert_ncbitaxon_ids";
//	public static final String STATUS_PROPERTY_BERT_NCBITAXON_SPANS_DONE = "bert_ncbitaxon_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_SO_IDS_DONE = "bert_so_ids";
//	public static final String STATUS_PROPERTY_BERT_SO_SPANS_DONE = "bert_so_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_PR_IDS_DONE = "bert_pr_ids";
//	public static final String STATUS_PROPERTY_BERT_PR_SPANS_DONE = "bert_pr_spans";
//	/**
//	 * 
//	 */
//	public static final String STATUS_PROPERTY_BERT_UBERON_IDS_DONE = "bert_uberon_ids";
//	public static final String STATUS_PROPERTY_BERT_UBERON_SPANS_DONE = "bert_uberon_spans";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE = "bigquery_file_export";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_PUBANNOTATION_FILE_EXPORT_DONE = "pubannotation_file_export";
//	/**
//	 * This property is for development convenience and allows a document to be
//	 * marked as a 'test' document. This can be helpful for testing purposes.
//	 */
//	public static final String STATUS_PROPERTY_TEST = "test";
	/**
	 * Array property to store Strings indicating the document collection to which
	 * the given document belongs, e.g. CORD-19
	 */
	public static final String STATUS_PROPERTY_COLLECTIONS = "collections";

//	public static final String STATUS_PROPERTY_IN_CORD19_COLLECTION = "in_cord19";
//	public static final String STATUS_PROPERTY_IN_PMCOA_COLLECTION = "in_pmcoa";
//	public static final String STATUS_PROPERTY_IN_NLM_MANUSCRIPT_COLLECTION = "in_nlm_manuscript";

}
