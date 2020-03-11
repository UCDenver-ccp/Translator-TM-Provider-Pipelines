package edu.cuanschutz.ccp.tm_provider.etl.util;

public interface DatastoreConstants {

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

	///////////////////////////////////////////////////////////////////////////
	////////////////////// FAILURE ENTITY PROPERTIES //////////////////////////
	///////////////////////////////////////////////////////////////////////////

	/**
	 * The Cloud Datastore entity type used for logging processing failures
	 */
	public static final String FAILURE_KIND = "failure";
	/**
	 * A unique id for a given failure = documentId.pipeline-key
	 */
	public static final String FAILURE_PROPERTY_ID = "id";
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
	public static final String FAILURE_PROPERTY_PIPELINE = "pipeline";

}
