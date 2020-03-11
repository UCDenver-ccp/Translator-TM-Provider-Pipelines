package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_MESSAGE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_PROPERTY_STACKTRACE;

import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;
import com.google.protobuf.ByteString;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Transforms a PCollection containing {@lnk EtlFailure} objects to a
 * PCollection containing Google Cloud Datastore Entities
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class EtlFailureToEntityFn extends DoFn<EtlFailureData, Entity> {

	private static final long serialVersionUID = 1L;
	private final PipelineKey pipelineKey;

	@ProcessElement
	public void processElement(@Element EtlFailureData failure, OutputReceiver<Entity> out)
			throws UnsupportedEncodingException {
		String docId = failure.getFileId();
		String message = failure.getMessage();
		String stackTrace = failure.getStackTrace();

		/* crop document id if it is a file path */
		docId = DocumentToEntityFn.updateDocId(docId);

		Entity entity = buildFailureEntity(docId, pipelineKey, message, stackTrace);
		out.output(entity);

	}

	static Entity buildFailureEntity(String docId, PipelineKey pipelineKey, String message, String stackTrace)
			throws UnsupportedEncodingException {
		String docName = docId + "." + pipelineKey.name().toLowerCase();
		Builder builder = Key.newBuilder();
		PathElement pathElement = builder.addPathBuilder().setKind(FAILURE_KIND).setName(docName).build();
		Key key = builder.setPath(0, pathElement).build();

		/*
		 * the stacktrace is likely too large to store as a property, so we make it a
		 * blob and store it unindexed
		 */
		ByteString stackTraceBlob = ByteString.copyFrom(stackTrace, CharacterEncoding.UTF_8.getCharacterSetName());
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(FAILURE_PROPERTY_ID, makeValue(docId).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_MESSAGE, makeValue(message).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_STACKTRACE,
				makeValue(stackTraceBlob).setExcludeFromIndexes(true).build());
		entityBuilder.putProperties(FAILURE_PROPERTY_PIPELINE, makeValue(pipelineKey.name()).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
