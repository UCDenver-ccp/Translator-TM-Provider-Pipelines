package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;

import org.apache.beam.sdk.transforms.DoFn;

import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A data structure summarizing the processing status for a given document
 */
@SuppressWarnings("rawtypes")
@EqualsAndHashCode(callSuper = false)
@Getter
public class ProcessingStatus extends DoFn {

	private static final long serialVersionUID = 1L;

	private final String documentId;
	private boolean textDone;
	private boolean dependencyParseDone;
	private boolean ogerChebiDone;
	private boolean ogerClDone;
	private boolean ogerGoBpDone;
	private boolean ogerGoCcDone;
	private boolean ogerGoMfDone;
	private boolean ogerMopDone;
	private boolean ogerNcbiTaxonDone;
	private boolean ogerPrDone;
	private boolean ogerSoDone;
	private boolean ogerUberonDone;
	private boolean bertChebiDone;
	private boolean bertClDone;
	private boolean bertGoBpDone;
	private boolean bertGoCcDone;
	private boolean bertGoMfDone;
	private boolean bertMopDone;
	private boolean bertNcbiTaxonDone;
	private boolean bertPrDone;
	private boolean bertSoDone;
	private boolean bertUberonDone;

	public ProcessingStatus(String documentId, EnumSet<ProcessingStatusFlag> flags) {
		this.documentId = documentId;
		for (ProcessingStatusFlag flag : flags) {
			enableFlag(flag);
		}
	}

	/**
	 * Sets the corresponding status flag to true
	 * 
	 * @param flag
	 */
	public void enableFlag(ProcessingStatusFlag flag) {
		toggleFlag(flag, true);
	}

	/**
	 * Sets the corresponding status flag to false
	 * 
	 * @param flag
	 */
	public void disableFlag(ProcessingStatusFlag flag) {
		toggleFlag(flag, false);
	}

	/**
	 * sets the specified status flag to the specified status (true/false)
	 * 
	 * @param flag
	 * @param status
	 */
	private void toggleFlag(ProcessingStatusFlag flag, boolean status) {
		switch (flag) {
		case TEXT_DONE:
			textDone = status;
			break;
		case DP_DONE:
			dependencyParseDone = status;
			break;

		////////////////////////////////////////////////////////////////
		/////////////////// OGER CONCEPT PROCESSING ////////////////////
		////////////////////////////////////////////////////////////////

		case OGER_CHEBI_DONE:
			ogerChebiDone = status;
			break;
		case OGER_CL_DONE:
			ogerClDone = status;
			break;
		case OGER_GO_BP_DONE:
			ogerGoBpDone = status;
			break;
		case OGER_GO_CC_DONE:
			ogerGoCcDone = status;
			break;
		case OGER_GO_MF_DONE:
			ogerGoMfDone = status;
			break;
		case OGER_MOP_DONE:
			ogerMopDone = status;
			break;
		case OGER_NCBITAXON_DONE:
			ogerNcbiTaxonDone = status;
			break;
		case OGER_PR_DONE:
			ogerPrDone = status;
			break;
		case OGER_SO_DONE:
			ogerSoDone = status;
			break;
		case OGER_UBERON_DONE:
			ogerUberonDone = status;
			break;

		////////////////////////////////////////////////////////////////
		////////////////// BERT CONCEPT PROCESSING /////////////////////
		////////////////////////////////////////////////////////////////

		case BERT_CHEBI_DONE:
			bertChebiDone = status;
			break;
		case BERT_CL_DONE:
			bertClDone = status;
			break;
		case BERT_GO_BP_DONE:
			bertGoBpDone = status;
			break;
		case BERT_GO_CC_DONE:
			bertGoCcDone = status;
			break;
		case BERT_GO_MF_DONE:
			bertGoMfDone = status;
			break;
		case BERT_MOP_DONE:
			bertMopDone = status;
			break;
		case BERT_NCBITAXON_DONE:
			bertNcbiTaxonDone = status;
			break;
		case BERT_PR_DONE:
			bertPrDone = status;
			break;
		case BERT_SO_DONE:
			bertSoDone = status;
			break;
		case BERT_UBERON_DONE:
			bertUberonDone = status;
			break;

		////////////////////////////////////////////////////////////////
		//////////////////// RELATION PROCESSING ///////////////////////
		////////////////////////////////////////////////////////////////

		// FAILURE IF THE FLAG IS NOT RECOGNIZED

		default:
			throw new IllegalArgumentException(String.format(
					"Unsupported Document Status Flag: %s. Code changes required to use this flag.", flag.name()));
		}
	}

}
