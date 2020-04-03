package edu.cuanschutz.ccp.tm_provider.etl.util;

import lombok.Getter;

public enum ProcessingStatusFlag {
	/**
	 * 
	 */
	TEXT_DONE(DatastoreConstants.STATUS_PROPERTY_TEXT_DONE),
	/**
	 * true if dependency parsing is complete
	 */
	DP_DONE(DatastoreConstants.STATUS_PROPERTY_DEPENDENCY_PARSE_DONE),

	////////////////////////////////////////////////////////////////
	/////////////////// OGER CONCEPT PROCESSING ////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
	OGER_CHEBI_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_CHEBI_DONE),
	/**
	 * 
	 */
	OGER_CL_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_CL_DONE),
	/**
	 * 
	 */
	OGER_GO_BP_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_GO_BP_DONE),
	/**
	 * 
	 */
	OGER_GO_CC_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_GO_CC_DONE),
	/**
	 * 
	 */
	OGER_GO_MF_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_GO_MF_DONE),
	/**
	 * 
	 */
	OGER_MOP_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_MOP_DONE),
	/**
	 * 
	 */
	OGER_NCBITAXON_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_NCBITAXON_DONE),
	/**
	 * 
	 */
	OGER_SO_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_SO_DONE),
	/**
	 * 
	 */
	OGER_PR_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_PR_DONE),
	/**
	 * 
	 */
	OGER_UBERON_DONE(DatastoreConstants.STATUS_PROPERTY_OGER_UBERON_DONE),

	////////////////////////////////////////////////////////////////
	////////////////// BERT CONCEPT PROCESSING /////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
	BERT_CHEBI_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_CHEBI_DONE),
	/**
	 * 
	 */
	BERT_CL_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_CL_DONE),
	/**
	 * 
	 */
	BERT_GO_BP_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_GO_BP_DONE),
	/**
	 * 
	 */
	BERT_GO_CC_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_GO_CC_DONE),
	/**
	 * 
	 */
	BERT_GO_MF_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_GO_MF_DONE),
	/**
	 * 
	 */
	BERT_MOP_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_MOP_DONE),
	/**
	 * 
	 */
	BERT_NCBITAXON_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_NCBITAXON_DONE),
	/**
	 * 
	 */
	BERT_SO_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_SO_DONE),
	/**
	 * 
	 */
	BERT_PR_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_PR_DONE),
	/**
	 * 
	 */
	BERT_UBERON_DONE(DatastoreConstants.STATUS_PROPERTY_BERT_UBERON_DONE),

	/**
	 * used to mark a failed process
	 */
	NOOP(null);
	TEST(DatastoreConstants.STATUS_PROPERTY_TEST),

	////////////////////////////////////////////////////////////////
	//////////////////// RELATION PROCESSING ///////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
//	ASSOC_CHEMICAL_PROTEIN_DONE

	@Getter
	private final String datastorePropertyName;

	private ProcessingStatusFlag(String datastorePropertyName) {
		this.datastorePropertyName = datastorePropertyName;
	}

}
