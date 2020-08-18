package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

public class PipelineTestUtil {

	public static Entity createEntity(String documentId, ProcessingStatusFlag... flagsToSet) {
		Key key = DatastoreKeyUtil.createStatusKey(documentId);
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(STATUS_PROPERTY_DOCUMENT_ID, makeValue(documentId).build());
		for (ProcessingStatusFlag flag : flagsToSet) {
			entityBuilder.putProperties(flag.getDatastoreFlagPropertyName(), makeValue(true).build());
		}
		Entity entity1 = entityBuilder.build();
		return entity1;
	}
}
