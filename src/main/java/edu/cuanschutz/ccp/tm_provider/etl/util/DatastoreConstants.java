package edu.cuanschutz.ccp.tm_provider.etl.util;

public interface DatastoreConstants {

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
	/**
	 * true if dependency parsing is complete
	 */
	public static final String STATUS_PROPERTY_DEPENDENCY_PARSE_DONE = "dep";

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

	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_CHEBI_DONE = "bert_chebi";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_CL_DONE = "bert_cl";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_GO_BP_DONE = "bert_gobp";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_GO_CC_DONE = "bert_gocc";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_GO_MF_DONE = "bert_gomf";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_MOP_DONE = "bert_mop";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_NCBITAXON_DONE = "bert_ncbitaxon";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_SO_DONE = "bert_so";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_PR_DONE = "bert_pr";
	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_BERT_UBERON_DONE = "bert_uberon";

}
