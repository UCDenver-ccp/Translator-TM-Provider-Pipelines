package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CHEBI_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CHEBI_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CL_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CL_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_BP_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_BP_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_CC_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_CC_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_MF_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_MF_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_MOP_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_MOP_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_NCBITAXON_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_NCBITAXON_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_PR_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_PR_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_SO_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_SO_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_UBERON_IDS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_UBERON_SPANS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DEPENDENCY_PARSE_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_CHEBI_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_CL_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_BP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_CC_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_MF_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_MOP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_NCBITAXON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_PR_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_SO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_UBERON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SECTIONS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_TEXT_DONE;

import lombok.Getter;

public enum ProcessingStatusFlag {
	/**
	 * 
	 */
	TEXT_DONE(STATUS_PROPERTY_TEXT_DONE),

	SECTIONS_DONE(STATUS_PROPERTY_SECTIONS_DONE),
	/**
	 * true if dependency parsing is complete
	 */
	DP_DONE(STATUS_PROPERTY_DEPENDENCY_PARSE_DONE),

	////////////////////////////////////////////////////////////////
	/////////////////// OGER CONCEPT PROCESSING ////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
	OGER_CHEBI_DONE(STATUS_PROPERTY_OGER_CHEBI_DONE),
	/**
	 * 
	 */
	OGER_CL_DONE(STATUS_PROPERTY_OGER_CL_DONE),
	/**
	 * 
	 */
	OGER_GO_BP_DONE(STATUS_PROPERTY_OGER_GO_BP_DONE),
	/**
	 * 
	 */
	OGER_GO_CC_DONE(STATUS_PROPERTY_OGER_GO_CC_DONE),
	/**
	 * 
	 */
	OGER_GO_MF_DONE(STATUS_PROPERTY_OGER_GO_MF_DONE),
	/**
	 * 
	 */
	OGER_MOP_DONE(STATUS_PROPERTY_OGER_MOP_DONE),
	/**
	 * 
	 */
	OGER_NCBITAXON_DONE(STATUS_PROPERTY_OGER_NCBITAXON_DONE),
	/**
	 * 
	 */
	OGER_SO_DONE(STATUS_PROPERTY_OGER_SO_DONE),
	/**
	 * 
	 */
	OGER_PR_DONE(STATUS_PROPERTY_OGER_PR_DONE),
	/**
	 * 
	 */
	OGER_UBERON_DONE(STATUS_PROPERTY_OGER_UBERON_DONE),

	////////////////////////////////////////////////////////////////
	////////////////// BERT CONCEPT PROCESSING /////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
	BERT_CHEBI_IDS_DONE(STATUS_PROPERTY_BERT_CHEBI_IDS_DONE),
	BERT_CHEBI_SPANS_DONE(STATUS_PROPERTY_BERT_CHEBI_SPANS_DONE),
	/**
	 * 
	 */
	BERT_CL_IDS_DONE(STATUS_PROPERTY_BERT_CL_IDS_DONE), BERT_CL_SPANS_DONE(STATUS_PROPERTY_BERT_CL_SPANS_DONE),
	/**
	 * 
	 */
	BERT_GO_BP_IDS_DONE(STATUS_PROPERTY_BERT_GO_BP_IDS_DONE),
	BERT_GO_BP_SPANS_DONE(STATUS_PROPERTY_BERT_GO_BP_SPANS_DONE),
	/**
	 * 
	 */
	BERT_GO_CC_IDS_DONE(STATUS_PROPERTY_BERT_GO_CC_IDS_DONE),
	BERT_GO_CC_SPANS_DONE(STATUS_PROPERTY_BERT_GO_CC_SPANS_DONE),
	/**
	 * 
	 */
	BERT_GO_MF_IDS_DONE(STATUS_PROPERTY_BERT_GO_MF_IDS_DONE),
	BERT_GO_MF_SPANS_DONE(STATUS_PROPERTY_BERT_GO_MF_SPANS_DONE),
	/**
	 * 
	 */
	BERT_MOP_IDS_DONE(STATUS_PROPERTY_BERT_MOP_IDS_DONE), BERT_MOP_SPANS_DONE(STATUS_PROPERTY_BERT_MOP_SPANS_DONE),
	/**
	 * 
	 */
	BERT_NCBITAXON_IDS_DONE(STATUS_PROPERTY_BERT_NCBITAXON_IDS_DONE),
	BERT_NCBITAXON_SPANS_DONE(STATUS_PROPERTY_BERT_NCBITAXON_SPANS_DONE),
	/**
	 * 
	 */
	BERT_SO_IDS_DONE(STATUS_PROPERTY_BERT_SO_IDS_DONE), BERT_SO_SPANS_DONE(STATUS_PROPERTY_BERT_SO_SPANS_DONE),
	/**
	 * 
	 */
	BERT_PR_IDS_DONE(STATUS_PROPERTY_BERT_PR_IDS_DONE), BERT_PR_SPANS_DONE(STATUS_PROPERTY_BERT_PR_SPANS_DONE),
	/**
	 * 
	 */
	BERT_UBERON_IDS_DONE(STATUS_PROPERTY_BERT_UBERON_IDS_DONE),
	BERT_UBERON_SPANS_DONE(STATUS_PROPERTY_BERT_UBERON_SPANS_DONE),

	/**
	 * used to mark a failed process
	 */
	NOOP(null),

	////////////////////////////////////////////////////////////////
	//////////////////// RELATION PROCESSING ///////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
//	ASSOC_CHEMICAL_PROTEIN_DONE

	BIGQUERY_LOAD_FILE_EXPORT_DONE(STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE);

	@Getter
	private final String datastoreFlagPropertyName;

	private ProcessingStatusFlag(String datastoreFlagPropertyName) {
		this.datastoreFlagPropertyName = datastoreFlagPropertyName;
	}

}
