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

	public static final String DOCUMENT_PROPERTY_COLLECTIONS = "collections";
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
	public static final String FAILURE_PROPERTY_CAUSE_MESSAGE = "cause_message";
	/**
	 * The stacktrace of the exception that was indicated for the failure
	 */
	public static final String FAILURE_PROPERTY_STACKTRACE = "stacktrace";
	public static final String FAILURE_PROPERTY_CAUSE_STACKTRACE = "cause_stacktrace";
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

	public static final String STATUS_PROPERTY_YEAR_PUBLISHED = "pub_year";

	public static final String STATUS_PROPERTY_PUBLICATION_TYPES = "pub_types";

	/**
	 * 
	 */
	public static final String STATUS_PROPERTY_TEXT_DONE = "text";

	public static final String STATUS_PROPERTY_TEXT_AUG_DONE = "text_aug";

	public static final String STATUS_PROPERTY_FILTER_UNACTIONABLE_TEXT_DONE = "filt_unact";

	public static final String STATUS_PROPERTY_SECTIONS_DONE = "sections";

	public static final String STATUS_PROPERTY_ABBREVIATIONS_DONE = "abbrev";
	/**
	 * true if dependency parsing is complete
	 */
	public static final String STATUS_PROPERTY_DEPENDENCY_PARSE_DONE = "dep";

	public static final String STATUS_PROPERTY_SENTENCE_SEGMENTATION_DONE = "sent";
	public static final String STATUS_PROPERTY_SENTENCE_SEGMENTATION_VIA_DEP_PARSE_DONE = "sent_by_dp";

	public static final String STATUS_PROPERTY_CONCEPT_POST_PROCESSING_DONE = "concept_pp";
	public static final String STATUS_PROPERTY_CONCEPT_POST_PROCESSING_UNFILTERED_DONE = "concept_pp_unfiltered";
	public static final String STATUS_PROPERTY_OGER_POST_PROCESSING_PART_1_DONE = "oger_pp1";
	public static final String STATUS_PROPERTY_OGER_POST_PROCESSING_PART_2_DONE = "oger_pp2";

	/**
	 * Concept cooccurrence count status
	 */
	public static final String STATUS_PROPERTY_CONCEPT_COOCCURRENCE_COUNTS_DONE = "concept_cooccur_counts";
//	public static final String STATUS_PROPERTY_CONCEPT_COOCCURRENCE_METRICS_DONE = "concept_cooccur_metrics";

	public static final String STATUS_PROPERTY_ELASTICSEARCH_INDEX_DONE = "elasticsearch";

	////////////////////////////////////////////////////////////////
	/////////////////// OGER CONCEPT PROCESSING ////////////////////
	////////////////////////////////////////////////////////////////

	public static final String STATUS_PROPERTY_OGER_DONE = "oger_done";

	public static final String STATUS_PROPERTY_OGER_CASE_SENSITIVE_DONE = "oger_cs";
	public static final String STATUS_PROPERTY_OGER_CASE_INSENSITIVE_MAX_NORM_DONE = "oger_cimax";
	public static final String STATUS_PROPERTY_OGER_CASE_INSENSITIVE_MIN_NORM_DONE = "oger_cimin";

	public static final String STATUS_PROPERTY_OGER_CHEBI_DONE = "oger_chebi";
	public static final String STATUS_PROPERTY_OGER_CL_DONE = "oger_cl";
	public static final String STATUS_PROPERTY_OGER_DRUGBANK_DONE = "oger_drugbank";
	public static final String STATUS_PROPERTY_OGER_GO_BP_DONE = "oger_gobp";
	public static final String STATUS_PROPERTY_OGER_GO_CC_DONE = "oger_gocc";
	public static final String STATUS_PROPERTY_OGER_GO_MF_DONE = "oger_gomf";
	public static final String STATUS_PROPERTY_OGER_HP_DONE = "oger_hp";
	public static final String STATUS_PROPERTY_OGER_MONDO_DONE = "oger_mondo";
	public static final String STATUS_PROPERTY_OGER_MOP_DONE = "oger_mop";
	public static final String STATUS_PROPERTY_OGER_MP_DONE = "oger_mp";
	public static final String STATUS_PROPERTY_OGER_NCBITAXON_DONE = "oger_ncbitaxon";
	public static final String STATUS_PROPERTY_OGER_SO_DONE = "oger_so";
	public static final String STATUS_PROPERTY_OGER_PR_DONE = "oger_pr";
	public static final String STATUS_PROPERTY_OGER_UBERON_DONE = "oger_uberon";

	////////////////////////////////////////////////////////////////
	/////////////////// CRF CONCEPT PROCESSING /////////////////////
	////////////////////////////////////////////////////////////////

	public static final String STATUS_PROPERTY_CRF_DONE = "crf_done";

//	public static final String STATUS_PROPERTY_CRF_CHEBI_DONE = "crf_chebi";
	public static final String STATUS_PROPERTY_CRF_NLMCHEM_DONE = "crf_nlmchem";
//	public static final String STATUS_PROPERTY_CRF_CL_DONE = "crf_cl";
//	public static final String STATUS_PROPERTY_CRF_GO_BP_DONE = "crf_gobp";
//	public static final String STATUS_PROPERTY_CRF_GO_CC_DONE = "crf_gocc";
//	public static final String STATUS_PROPERTY_CRF_GO_MF_DONE = "crf_gomf";
	public static final String STATUS_PROPERTY_CRF_HP_DONE = "crf_hp";
//	public static final String STATUS_PROPERTY_CRF_MONDO_DONE = "crf_mondo";
//	public static final String STATUS_PROPERTY_CRF_MOP_DONE = "crf_mop";
//	public static final String STATUS_PROPERTY_CRF_NCBITAXON_DONE = "crf_ncbitaxon";
//	public static final String STATUS_PROPERTY_CRF_SO_DONE = "crf_so";
//	public static final String STATUS_PROPERTY_CRF_PR_DONE = "crf_pr";
//	public static final String STATUS_PROPERTY_CRF_UBERON_DONE = "crf_uberon";
	public static final String STATUS_PROPERTY_CRF_CRAFT_DONE = "crf_craft";
	public static final String STATUS_PROPERTY_CRF_NLMDISEASE_DONE = "crf_nlm_disease";

	////////////////////////////////////////////////////////////////
	///////////////////// SENTENCE EXPORT //////////////////////////
	////////////////////////////////////////////////////////////////

	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION_DONE = "sent_xprt_blgtesa";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION_DONE = "sent_xprt_bldtpfa";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION_DONE = "sent_xprt_blctdopfa";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_DONE = "sent_xprt_blgtda";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_LOSS_GAIN_FUNCTION_DONE = "sent_xprt_blgtda_lfgf";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION_DONE = "sent_xprt_blgrra";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_GENE_ASSOCIATION_DONE = "sent_xprt_blctga";
	public static final String STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_GO_TERM_ASSOCIATION_DONE = "sent_xprt_blgtgta";

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

	public static final String STATUS_PROPERTY_SENTENCE_COOCCURRENCE_EXPORT_DONE = "sent_cooccur_export";

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

	public static final String STATUS_PROPERTY_TEXT_EXTRACTION_DONE = "text_extract";
	public static final String STATUS_PROPERTY_CONLL03_DONE = "conll03";

//	public static final String STATUS_PROPERTY_IN_CORD19_COLLECTION = "in_cord19";
//	public static final String STATUS_PROPERTY_IN_PMCOA_COLLECTION = "in_pmcoa";
//	public static final String STATUS_PROPERTY_IN_NLM_MANUSCRIPT_COLLECTION = "in_nlm_manuscript";

}
