package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_DOCUMENT_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_DOCUMENT_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_MESSAGE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_PIPELINE_VERSION;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_STACKTRACE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_TIMESTAMP;

import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.protobuf.ByteString;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;

/**
 * Transforms a PCollection containing {@lnk EtlFailure} objects to a
 * PCollection containing Google Cloud Datastore Entities
 *
 */
public class EtlFailureToEntityFn extends DoFn<EtlFailureData, Entity> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(@Element EtlFailureData failure, OutputReceiver<Entity> out)
			throws UnsupportedEncodingException {
		String docId = failure.getDocumentId();
		String message = failure.getMessage();
		String stackTrace = failure.getStackTrace();
		PipelineKey pipeline = failure.getPipeline();
		String pipelineVersion = failure.getPipelineVersion();
		DocumentType documentType = failure.getDocumentType();
		com.google.cloud.Timestamp timestamp = failure.getTimestamp();

		/* crop document id if it is a file path */
		docId = DocumentToEntityFn.updateDocId(docId);

		Entity entity = buildFailureEntity(pipeline, pipelineVersion, documentType, docId, message, stackTrace,
				timestamp);
		out.output(entity);

	}

	static Entity buildFailureEntity(PipelineKey pipeline, String pipelineVersion, DocumentType documentType,
			String docId, String message, String stackTrace, com.google.cloud.Timestamp timestamp)
			throws UnsupportedEncodingException {
		Key key = DatastoreKeyUtil.createFailureKey(pipeline, pipelineVersion, documentType, docId);

		/*
		 * the stacktrace is likely too large to store as a property, so we make it a
		 * blob and store it unindexed
		 */
		ByteString stackTraceBlob = ByteString.copyFrom(stackTrace, CharacterEncoding.UTF_8.getCharacterSetName());
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(FAILURE_PROPERTY_PIPELINE, makeValue(pipeline.name()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_PIPELINE_VERSION, makeValue(pipelineVersion).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_DOCUMENT_TYPE, makeValue(documentType.name()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_DOCUMENT_ID, makeValue(docId).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_TIMESTAMP, makeValue(timestamp.toString()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_MESSAGE, makeValue(message).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_STACKTRACE,
				makeValue(stackTraceBlob).setExcludeFromIndexes(true).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
