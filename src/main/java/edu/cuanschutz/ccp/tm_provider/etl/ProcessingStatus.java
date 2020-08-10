package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A data structure summarizing the processing status for a given document
 */
@SuppressWarnings("rawtypes")
@EqualsAndHashCode(callSuper = false)
public class ProcessingStatus extends DoFn {

	private static final long serialVersionUID = 1L;

	@Getter
	private final String documentId;

	private Map<String, Boolean> flagPropertiesMap;
	private Map<String, Long> countPropertiesMap;

//	private Map<ProcessingStatusFlag, String> flagToFlagPropertyNameMap;
//	private Map<ProcessingStatusFlag, String> flagToCountPropertyNameMap;

//	private boolean textDone;
//	private int textChunkCount;
//	private boolean dependencyParseDone;
//	private int dependencyParseChunkCount;
//	private boolean ogerChebiDone;
//	private int ogerChebiChunkCount;
//	private boolean ogerClDone;
//	private int ogerClChunkCount;
//	private boolean ogerGoBpDone;
//	private int ogerGoBpChunkCount;
//	private boolean ogerGoCcDone;
//	private int ogerGoCcChunkCount;
//	private boolean ogerGoMfDone;
//	private int ogerGoMfChunkCount;
//	private boolean ogerMopDone;
//	private int ogerMopChunkCount;
//	private boolean ogerNcbiTaxonDone;
//	private int ogerNcbiTaxonChunkCount;
//	private boolean ogerPrDone;
//	private int ogerPrChunkCount;
//	private boolean ogerSoDone;
//	private int ogerSoChunkCount;
//	private boolean ogerUberonDone;
//	private int ogerUberonChunkCount;
//
//	// BIOBERT RUNS (to work in combination with the OGER runs)
//	private boolean bertChebiIdsDone;
//	private int bertChebiIdsChunkCount;
//	private boolean bertChebiSpansDone;
//	private int bertChebiSpansChunkCount;
//	private boolean bertClIdsDone;
//	private int bertClIdsChunkCount;
//	private boolean bertClSpansDone;
//	private int bertClSpansChunkCount;
//	private boolean bertGoBpIdsDone;
//	private int bertGoBpIdsChunkCount;
//	private boolean bertGoBpSpansDone;
//	private int bertGoBpSpansChunkCount;
//	private boolean bertGoCcIdsDone;
//	private int bertGoCcIdsChunkCount;
//	private boolean bertGoCcSpansDone;
//	private int bertGoCcSpansChunkCount;
//	private boolean bertGoMfIdsDone;
//	private int bertGoMfIdsChunkCount;
//	private boolean bertGoMfSpansDone;
//	private int bertGoMfSpansChunkCount;
//	private boolean bertMopIdsDone;
//	private int bertMopIdsChunkCount;
//	private boolean bertMopSpansDone;
//	private int bertMopSpansChunkCount;
//	private boolean bertNcbiTaxonIdsDone;
//	private int bertNcbiTaxonIdsChunkCount;
//	private boolean bertNcbiTaxonSpansDone;
//	private int bertNcbiTaxonSpansChunkCount;
//	private boolean bertPrIdsDone;
//	private int bertPrIdsChunkCount;
//	private boolean bertPrSpansDone;
//	private int bertPrSpansChunkCount;
//	private boolean bertSoIdsDone;
//	private int bertSoIdsChunkCount;
//	private boolean bertSoSpansDone;
//	private int bertSoSpansChunkCount;
//	private boolean bertUberonIdsDone;
//	private int bertUberonIdsChunkCount;
//	private boolean bertUberonSpansDone;
//	private int bertUberonSpansChunkCount;
//
//	private boolean bigqueryExportDone;
//	private int bigqueryExportChunkCount;
//
//	private boolean isTest;
	@Getter
	private Set<String> collections;

	public ProcessingStatus(String documentId) {
		this.documentId = documentId;
		this.flagPropertiesMap = new HashMap<String, Boolean>();
		this.countPropertiesMap = new HashMap<String, Long>();
	}

	public void addCollection(String collectionName) {
		if (this.collections == null) {
			this.collections = new HashSet<String>();
		}
		this.collections.add(collectionName);
	}

	public boolean getFlagPropertyValue(String property) {
		if (flagPropertiesMap.containsKey(property)) {
			return flagPropertiesMap.get(property);
		}
		return false;
	}

	public long getCountPropertyValue(String property) {
		if (countPropertiesMap.containsKey(property)) {
			return countPropertiesMap.get(property);
		}
		return -1;
	}

	public Set<String> getFlagProperties() {
		return new HashSet<String>(flagPropertiesMap.keySet());
	}

	public Set<String> getCountProperties() {
		return new HashSet<String>(countPropertiesMap.keySet());
	}

	public void setFlagProperty(String property, boolean value) {
		this.flagPropertiesMap.put(property, value);
	}

	public void setCountProperty(String property, long value) {
		this.countPropertiesMap.put(property, value);
	}

	/**
	 * Sets the corresponding status flag to true
	 * 
	 * @param flag
	 */
	public void enableFlag(ProcessingStatusFlag flag, DocumentCriteria dc, int correspondingChunkCount) {
		toggleFlag(flag, true, dc, correspondingChunkCount);
	}

	/**
	 * Sets the corresponding status flag to false
	 * 
	 * @param flag
	 */
	public void disableFlag(ProcessingStatusFlag flag, DocumentCriteria dc) {
		toggleFlag(flag, false, dc, -1);
	}

	/**
	 * sets the specified status flag to the specified status (true/false)
	 * 
	 * @param flag
	 * @param status
	 */
	private void toggleFlag(ProcessingStatusFlag flag, boolean status, DocumentCriteria dc,
			long correspondingChunkCount) {
		if (flag != ProcessingStatusFlag.NOOP) {
			String flagPropertyName = flag.getDatastoreFlagPropertyName();

			/*
			 * the chunk count property name includes the document type, format, pipeline
			 * key, and pipeline version so that we can catalog different runs if needed.
			 */
			String chunkCountPropertyName = DatastoreProcessingStatusUtil.getDocumentChunkCountPropertyName(dc);

			flagPropertiesMap.put(flagPropertyName, status);
			countPropertiesMap.put(chunkCountPropertyName, correspondingChunkCount);
		}
	}

//		switch (flag) {
//		case TEXT_DONE:
//			
//			textDone = status;
//			textChunkCount = correspondingChunkCount;
//			break;
//		case DP_DONE:
//			dependencyParseDone = status;
//			dependencyParseChunkCount = correspondingChunkCount;
//			break;
//
//		////////////////////////////////////////////////////////////////
//		/////////////////// OGER CONCEPT PROCESSING ////////////////////
//		////////////////////////////////////////////////////////////////
//
//		case OGER_CHEBI_DONE:
//			ogerChebiDone = status;
//			ogerChebiChunkCount = correspondingChunkCount;
//			break;
//		case OGER_CL_DONE:
//			ogerClDone = status;
//			ogerClChunkCount = correspondingChunkCount;
//			break;
//		case OGER_GO_BP_DONE:
//			ogerGoBpDone = status;
//			ogerGoBpChunkCount = correspondingChunkCount;
//			break;
//		case OGER_GO_CC_DONE:
//			ogerGoCcDone = status;
//			ogerGoCcChunkCount = correspondingChunkCount;
//			break;
//		case OGER_GO_MF_DONE:
//			ogerGoMfDone = status;
//			ogerGoMfChunkCount = correspondingChunkCount;
//			break;
//		case OGER_MOP_DONE:
//			ogerMopDone = status;
//			ogerMopChunkCount = correspondingChunkCount;
//			break;
//		case OGER_NCBITAXON_DONE:
//			ogerNcbiTaxonDone = status;
//			ogerNcbiTaxonChunkCount = correspondingChunkCount;
//			break;
//		case OGER_PR_DONE:
//			ogerPrDone = status;
//			ogerPrChunkCount = correspondingChunkCount;
//			break;
//		case OGER_SO_DONE:
//			ogerSoDone = status;
//			ogerSoChunkCount = correspondingChunkCount;
//			break;
//		case OGER_UBERON_DONE:
//			ogerUberonDone = status;
//			ogerUberonChunkCount = correspondingChunkCount;
//			break;
//
//		////////////////////////////////////////////////////////////////
//		////////////////// BERT CONCEPT PROCESSING /////////////////////
//		////////////////////////////////////////////////////////////////
//
//		case BERT_CHEBI_IDS_DONE:
//			bertChebiIdsDone = status;
//			bertChebiIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_CHEBI_SPANS_DONE:
//			bertChebiSpansDone = status;
//			bertChebiSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_CL_IDS_DONE:
//			bertClIdsDone = status;
//			bertClIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_CL_SPANS_DONE:
//			bertClSpansDone = status;
//			bertClSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_BP_IDS_DONE:
//			bertGoBpIdsDone = status;
//			bertGoBpIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_BP_SPANS_DONE:
//			bertGoBpSpansDone = status;
//			bertGoBpSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_CC_IDS_DONE:
//			bertGoCcIdsDone = status;
//			bertGoCcIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_CC_SPANS_DONE:
//			bertGoCcSpansDone = status;
//			bertGoCcSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_MF_IDS_DONE:
//			bertGoMfIdsDone = status;
//			bertGoMfIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_GO_MF_SPANS_DONE:
//			bertGoMfSpansDone = status;
//			bertGoMfSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_MOP_IDS_DONE:
//			bertMopIdsDone = status;
//			bertMopIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_MOP_SPANS_DONE:
//			bertMopSpansDone = status;
//			bertMopSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_NCBITAXON_IDS_DONE:
//			bertNcbiTaxonIdsDone = status;
//			bertNcbiTaxonIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_NCBITAXON_SPANS_DONE:
//			bertNcbiTaxonSpansDone = status;
//			bertNcbiTaxonSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_PR_IDS_DONE:
//			bertPrIdsDone = status;
//			bertPrIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_PR_SPANS_DONE:
//			bertPrSpansDone = status;
//			bertPrSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_SO_IDS_DONE:
//			bertSoIdsDone = status;
//			bertSoIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_SO_SPANS_DONE:
//			bertSoSpansDone = status;
//			bertSoSpansChunkCount = correspondingChunkCount;
//			break;
//		case BERT_UBERON_IDS_DONE:
//			bertUberonIdsDone = status;
//			bertUberonIdsChunkCount = correspondingChunkCount;
//			break;
//		case BERT_UBERON_SPANS_DONE:
//			bertUberonSpansDone = status;
//			bertUberonSpansChunkCount = correspondingChunkCount;
//			break;
//
//		////////////////////////////////////////////////////////////////
//		//////////////////// RELATION PROCESSING ///////////////////////
//		////////////////////////////////////////////////////////////////
//
//		// FAILURE IF THE FLAG IS NOT RECOGNIZED
//		case BIGQUERY_LOAD_FILE_EXPORT_DONE:
//			bigqueryExportDone = status;
//			bigqueryExportChunkCount = correspondingChunkCount;
//			break;
//
//		case TEST:
//			isTest = status;
//			break;
//
//		case NOOP:
//			// do nothing
//			break;
//
//		default:
//			throw new IllegalArgumentException(String.format(
//					"Unsupported Document Status Flag: %s. Code changes required to use this flag.", flag.name()));
//		}
//	}

//	/**
//	 * Given the document type and pipeline key, return the chunk count for the
//	 * stored document. The chunk count refers to the number of chunks the document
//	 * was divided into in order to store it in Datastore while complying with the
//	 * Datastore max byte threshold.
//	 * 
//	 * @param documentType
//	 * @param pipelineKey
//	 * @return
//	 */
//	public int getChunkCount(DocumentType documentType, PipelineKey pipelineKey) {
//		if (documentType == DocumentType.TEXT) {
//			return textChunkCount;
//		}
//		if (documentType == DocumentType.DEPENDENCY_PARSE) {
//			return dependencyParseChunkCount;
//		}
//
//		////////////////////////////////////////////////////////////////
//		/////////////////// OGER CONCEPT PROCESSING ////////////////////
//		////////////////////////////////////////////////////////////////
//
//		if (pipelineKey == PipelineKey.OGER) {
//			switch (documentType) {
//			case CONCEPT_CHEBI:
//				return ogerChebiChunkCount;
//			case CONCEPT_CL:
//				return ogerClChunkCount;
//			case CONCEPT_GO_BP:
//				return ogerGoBpChunkCount;
//			case CONCEPT_GO_CC:
//				return ogerGoCcChunkCount;
//			case CONCEPT_GO_MF:
//				return ogerGoMfChunkCount;
//			case CONCEPT_MOP:
//				return ogerMopChunkCount;
//			case CONCEPT_NCBITAXON:
//				return ogerNcbiTaxonChunkCount;
//			case CONCEPT_PR:
//				return ogerPrChunkCount;
//			case CONCEPT_SO:
//				return ogerSoChunkCount;
//			case CONCEPT_UBERON:
//				return ogerUberonChunkCount;
//			default:
//				throw new IllegalArgumentException("Invalid document criteria pairing -- document type: " + documentType
//						+ " & pipeline key: " + pipelineKey);
//			}
//		}
//
//		if (pipelineKey == PipelineKey.BERT_IDS) {
//			switch (documentType) {
//			case CONCEPT_CHEBI:
//				return bertChebiIdsChunkCount;
//			case CONCEPT_CL:
//				return bertClIdsChunkCount;
//			case CONCEPT_GO_BP:
//				return bertGoBpIdsChunkCount;
//			case CONCEPT_GO_CC:
//				return bertGoCcIdsChunkCount;
//			case CONCEPT_GO_MF:
//				return bertGoMfIdsChunkCount;
//			case CONCEPT_MOP:
//				return bertMopIdsChunkCount;
//			case CONCEPT_NCBITAXON:
//				return bertNcbiTaxonIdsChunkCount;
//			case CONCEPT_PR:
//				return bertPrIdsChunkCount;
//			case CONCEPT_SO:
//				return bertSoIdsChunkCount;
//			case CONCEPT_UBERON:
//				return bertUberonIdsChunkCount;
//			default:
//				throw new IllegalArgumentException(
//						String.format("Invalid document criteria pairing -- document type: %s & pipeline key: %s",
//								documentType.name(), pipelineKey.name()));
//			}
//		}
//
//		if (pipelineKey == PipelineKey.BERT_SPANS) {
//			switch (documentType) {
//			case CONCEPT_CHEBI:
//				return bertChebiSpansChunkCount;
//			case CONCEPT_CL:
//				return bertClSpansChunkCount;
//			case CONCEPT_GO_BP:
//				return bertGoBpSpansChunkCount;
//			case CONCEPT_GO_CC:
//				return bertGoCcSpansChunkCount;
//			case CONCEPT_GO_MF:
//				return bertGoMfSpansChunkCount;
//			case CONCEPT_MOP:
//				return bertMopSpansChunkCount;
//			case CONCEPT_NCBITAXON:
//				return bertNcbiTaxonSpansChunkCount;
//			case CONCEPT_PR:
//				return bertPrSpansChunkCount;
//			case CONCEPT_SO:
//				return bertSoSpansChunkCount;
//			case CONCEPT_UBERON:
//				return bertUberonSpansChunkCount;
//			default:
//				throw new IllegalArgumentException(
//						String.format("Invalid document criteria pairing -- document type: %s & pipeline key: %s",
//								documentType.name(), pipelineKey.name()));
//			}
//		}
//
//		throw new IllegalArgumentException(String.format(
//				"Unsupported Document Criteria. No chunk count for -- document type: %s & pipeline key: %s",
//				documentType.name(), pipelineKey.name()));
//	}
}
