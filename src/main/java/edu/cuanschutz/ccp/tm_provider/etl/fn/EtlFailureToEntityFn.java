package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_DOCUMENT_FORMAT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_DOCUMENT_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_DOCUMENT_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_MESSAGE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_PIPELINE_VERSION;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_STACKTRACE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_TIMESTAMP;

import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.protobuf.ByteString;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;

/**
 * Transforms a PCollection containing {@lnk EtlFailure} objects to a
 * PCollection containing Google Cloud Datastore Entities
 *
 */
public class EtlFailureToEntityFn extends DoFn<EtlFailureData, KV<String, Entity>> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(@Element EtlFailureData failure, OutputReceiver<KV<String, Entity>> out)
			throws UnsupportedEncodingException {
		String docId = failure.getDocumentId();
		String message = failure.getMessage();
		String stackTrace = failure.getStackTrace();
		PipelineKey pipeline = failure.getDocumentCriteria().getPipelineKey();
		String pipelineVersion = failure.getDocumentCriteria().getPipelineVersion();
		DocumentType documentType = failure.getDocumentCriteria().getDocumentType();
		DocumentFormat documentFormat = failure.getDocumentCriteria().getDocumentFormat();
		com.google.cloud.Timestamp timestamp = failure.getTimestamp();

		/* crop document id if it is a file path */
		docId = DocumentToEntityFn.updateDocId(docId);

		DocumentCriteria dc = new DocumentCriteria(documentType, documentFormat, pipeline, pipelineVersion);

		Entity entity = buildFailureEntity(dc, docId, message, stackTrace, timestamp);
		out.output(KV.of(entity.getKey().toString(), entity));

	}

	static Entity buildFailureEntity(DocumentCriteria dc, String docId, String message, String stackTrace,
			com.google.cloud.Timestamp timestamp) throws UnsupportedEncodingException {
		Key key = DatastoreKeyUtil.createFailureKey(docId, dc);

		/*
		 * the stacktrace is likely too large to store as a property, so we make it a
		 * blob and store it unindexed
		 */
		ByteString stackTraceBlob = ByteString.copyFrom(stackTrace, CharacterEncoding.UTF_8.getCharacterSetName());
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(FAILURE_PROPERTY_PIPELINE, makeValue(dc.getPipelineKey().name()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_PIPELINE_VERSION, makeValue(dc.getPipelineVersion()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_DOCUMENT_TYPE, makeValue(dc.getDocumentType().name()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_DOCUMENT_FORMAT, makeValue(dc.getDocumentFormat().name()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_DOCUMENT_ID, makeValue(docId).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_TIMESTAMP, makeValue(timestamp.toString()).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_MESSAGE, makeValue(message).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_STACKTRACE,
				makeValue(stackTraceBlob).setExcludeFromIndexes(true).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
