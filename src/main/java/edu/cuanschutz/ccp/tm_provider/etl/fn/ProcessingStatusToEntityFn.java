package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CHEBI_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_CL_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_BP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_CC_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_GO_MF_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_MOP_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_NCBITAXON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_PR_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_SO_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_BERT_UBERON_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DEPENDENCY_PARSE_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;
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
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_TEXT_DONE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_TEST;

import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Transforms a PCollection containing {@lnk ProcessingStatus} objects to a
 * PCollection containing Google Cloud Datastore Entities
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ProcessingStatusToEntityFn extends DoFn<ProcessingStatus, Entity> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(@Element ProcessingStatus status, OutputReceiver<Entity> out)
			throws UnsupportedEncodingException {
		Entity entity = buildStatusEntity(status);
		out.output(entity);

	}

	static Entity buildStatusEntity(ProcessingStatus status) throws UnsupportedEncodingException {
		Key key = DatastoreKeyUtil.createStatusKey(status.getDocumentId());

		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(STATUS_PROPERTY_DOCUMENT_ID, makeValue(status.getDocumentId()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_TEXT_DONE, makeValue(status.isTextDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_DEPENDENCY_PARSE_DONE,
				makeValue(status.isDependencyParseDone()).build());
		// OGER concept processing status
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_CHEBI_DONE, makeValue(status.isOgerChebiDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_CL_DONE, makeValue(status.isOgerClDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_GO_BP_DONE, makeValue(status.isOgerGoBpDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_GO_CC_DONE, makeValue(status.isOgerGoCcDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_GO_MF_DONE, makeValue(status.isOgerGoMfDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_MOP_DONE, makeValue(status.isOgerMopDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_NCBITAXON_DONE,
				makeValue(status.isOgerNcbiTaxonDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_PR_DONE, makeValue(status.isOgerPrDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_SO_DONE, makeValue(status.isOgerSoDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_OGER_UBERON_DONE, makeValue(status.isOgerUberonDone()).build());
		// BERT concept processing status
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_CHEBI_DONE, makeValue(status.isBertChebiDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_CL_DONE, makeValue(status.isBertClDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_GO_BP_DONE, makeValue(status.isBertGoBpDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_GO_CC_DONE, makeValue(status.isBertGoCcDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_GO_MF_DONE, makeValue(status.isBertGoMfDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_MOP_DONE, makeValue(status.isBertMopDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_NCBITAXON_DONE,
				makeValue(status.isBertNcbiTaxonDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_PR_DONE, makeValue(status.isBertPrDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_SO_DONE, makeValue(status.isBertSoDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_BERT_UBERON_DONE, makeValue(status.isBertUberonDone()).build());
		entityBuilder.putProperties(STATUS_PROPERTY_TEST,
				makeValue(status.isTest()).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
