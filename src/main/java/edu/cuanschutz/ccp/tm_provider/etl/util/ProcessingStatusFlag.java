package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CONCEPT_COOCCURRENCE_COUNTS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CONCEPT_POST_PROCESSING_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CONCEPT_POST_PROCESSING_UNFILTERED_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_CHEBI_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_CL_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_GO_BP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_GO_CC_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_GO_MF_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_HP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_MONDO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_MOP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_NCBITAXON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_NLMCHEM_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_PR_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_SO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_CRF_UBERON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DEPENDENCY_PARSE_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_ELASTICSEARCH_INDEX_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_CHEBI_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_CL_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_DRUGBANK_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_BP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_CC_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_GO_MF_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_HP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_MONDO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_MOP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_MP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_NCBITAXON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_PR_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_SO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_OGER_UBERON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_PUBANNOTATION_FILE_EXPORT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SECTIONS_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_COOCCURRENCE_EXPORT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_GENE_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_LOSS_GAIN_FUNCTION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_GO_TERM_ASSOCIATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_SENTENCE_SEGMENTATION_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_TEXT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_ABBREVIATIONS_DONE;

import lombok.Getter;

public enum ProcessingStatusFlag {
	/**
	 * 
	 */
	TEXT_DONE(STATUS_PROPERTY_TEXT_DONE),

	SECTIONS_DONE(STATUS_PROPERTY_SECTIONS_DONE),
	
	ABBREVIATIONS_DONE(STATUS_PROPERTY_ABBREVIATIONS_DONE),
	/**
	 * true if dependency parsing is complete
	 */
	DP_DONE(STATUS_PROPERTY_DEPENDENCY_PARSE_DONE),

	SENTENCE_DONE(STATUS_PROPERTY_SENTENCE_SEGMENTATION_DONE),

	CONCEPT_POST_PROCESSING_DONE(STATUS_PROPERTY_CONCEPT_POST_PROCESSING_DONE),

	CONCEPT_POST_PROCESSING_UNFILTERED_DONE(STATUS_PROPERTY_CONCEPT_POST_PROCESSING_UNFILTERED_DONE),

	CONCEPT_COOCCURRENCE_COUNTS_DONE(STATUS_PROPERTY_CONCEPT_COOCCURRENCE_COUNTS_DONE),

	ELASTICSEARCH_INDEX_DONE(STATUS_PROPERTY_ELASTICSEARCH_INDEX_DONE),

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

	OGER_DRUGBANK_DONE(STATUS_PROPERTY_OGER_DRUGBANK_DONE),
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

	OGER_HP_DONE(STATUS_PROPERTY_OGER_HP_DONE),

	OGER_MONDO_DONE(STATUS_PROPERTY_OGER_MONDO_DONE),
	/**
	 * 
	 */
	OGER_MOP_DONE(STATUS_PROPERTY_OGER_MOP_DONE),

	OGER_MP_DONE(STATUS_PROPERTY_OGER_MP_DONE),
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
/////////////////// CRF CONCEPT PROCESSING ////////////////////
////////////////////////////////////////////////////////////////

	/**
	* 
	*/
	CRF_CHEBI_DONE(STATUS_PROPERTY_CRF_CHEBI_DONE), CRF_NLMCHEM_DONE(STATUS_PROPERTY_CRF_NLMCHEM_DONE),
	/**
	* 
	*/
	CRF_CL_DONE(STATUS_PROPERTY_CRF_CL_DONE),
	/**
	* 
	*/
	CRF_GO_BP_DONE(STATUS_PROPERTY_CRF_GO_BP_DONE),
	/**
	* 
	*/
	CRF_GO_CC_DONE(STATUS_PROPERTY_CRF_GO_CC_DONE),
	/**
	* 
	*/
	CRF_GO_MF_DONE(STATUS_PROPERTY_CRF_GO_MF_DONE),

	CRF_HP_DONE(STATUS_PROPERTY_CRF_HP_DONE),

	CRF_MONDO_DONE(STATUS_PROPERTY_CRF_MONDO_DONE),
	/**
	* 
	*/
	CRF_MOP_DONE(STATUS_PROPERTY_CRF_MOP_DONE),
	/**
	* 
	*/
	CRF_NCBITAXON_DONE(STATUS_PROPERTY_CRF_NCBITAXON_DONE),
	/**
	* 
	*/
	CRF_SO_DONE(STATUS_PROPERTY_CRF_SO_DONE),
	/**
	* 
	*/
	CRF_PR_DONE(STATUS_PROPERTY_CRF_PR_DONE),
	/**
	* 
	*/
	CRF_UBERON_DONE(STATUS_PROPERTY_CRF_UBERON_DONE),

	////////////////////////////////////////////////////////////////
	////////////////// BERT CONCEPT PROCESSING /////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * 
	 */
//	BERT_CHEBI_IDS_DONE(STATUS_PROPERTY_BERT_CHEBI_IDS_DONE),
//	BERT_CHEBI_SPANS_DONE(STATUS_PROPERTY_BERT_CHEBI_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_CL_IDS_DONE(STATUS_PROPERTY_BERT_CL_IDS_DONE), BERT_CL_SPANS_DONE(STATUS_PROPERTY_BERT_CL_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_GO_BP_IDS_DONE(STATUS_PROPERTY_BERT_GO_BP_IDS_DONE),
//	BERT_GO_BP_SPANS_DONE(STATUS_PROPERTY_BERT_GO_BP_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_GO_CC_IDS_DONE(STATUS_PROPERTY_BERT_GO_CC_IDS_DONE),
//	BERT_GO_CC_SPANS_DONE(STATUS_PROPERTY_BERT_GO_CC_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_GO_MF_IDS_DONE(STATUS_PROPERTY_BERT_GO_MF_IDS_DONE),
//	BERT_GO_MF_SPANS_DONE(STATUS_PROPERTY_BERT_GO_MF_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_MOP_IDS_DONE(STATUS_PROPERTY_BERT_MOP_IDS_DONE), BERT_MOP_SPANS_DONE(STATUS_PROPERTY_BERT_MOP_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_NCBITAXON_IDS_DONE(STATUS_PROPERTY_BERT_NCBITAXON_IDS_DONE),
//	BERT_NCBITAXON_SPANS_DONE(STATUS_PROPERTY_BERT_NCBITAXON_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_SO_IDS_DONE(STATUS_PROPERTY_BERT_SO_IDS_DONE), BERT_SO_SPANS_DONE(STATUS_PROPERTY_BERT_SO_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_PR_IDS_DONE(STATUS_PROPERTY_BERT_PR_IDS_DONE), BERT_PR_SPANS_DONE(STATUS_PROPERTY_BERT_PR_SPANS_DONE),
//	/**
//	 * 
//	 */
//	BERT_UBERON_IDS_DONE(STATUS_PROPERTY_BERT_UBERON_IDS_DONE),
//	BERT_UBERON_SPANS_DONE(STATUS_PROPERTY_BERT_UBERON_SPANS_DONE),

	/**
	 * used to mark a failed process
	 */
	NOOP(null),

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////// RELATION PROCESSING - SENTENCE OUTPUT
	////////////////////////////////////////////////////////////////////////////////// ///////////////////////
	//////////////////////////////////////////////////////////////////////////////////

	SENTENCE_EXPORT_BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_GENE_TO_DISEASE_LOSS_GAIN_FUNCTION_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_DISEASE_ASSOCIATION_LOSS_GAIN_FUNCTION_DONE),
	SENTENCE_EXPORT_BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_CHEMICAL_TO_GENE_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_CHEMICAL_TO_GENE_ASSOCIATION_DONE),
	SENTENCE_EXPORT_BL_GENE_TO_GO_TERM_ASSOCIATION_DONE(
			STATUS_PROPERTY_SENTENCE_EXPORT_BL_GENE_TO_GO_TERM_ASSOCIATION_DONE),

	/**
	 * 
	 */
//	ASSOC_CHEMICAL_PROTEIN_DONE

	BIGQUERY_LOAD_FILE_EXPORT_DONE(STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE),

	/**
	 * 
	 */
	PUBANNOTATION_FILE_EXPORT_DONE(STATUS_PROPERTY_PUBANNOTATION_FILE_EXPORT_DONE),

	SENTENCE_COOCCURRENCE_EXPORT_DONE(STATUS_PROPERTY_SENTENCE_COOCCURRENCE_EXPORT_DONE);

	@Getter
	private final String datastoreFlagPropertyName;

	private ProcessingStatusFlag(String datastoreFlagPropertyName) {
		this.datastoreFlagPropertyName = datastoreFlagPropertyName;
	}

}
