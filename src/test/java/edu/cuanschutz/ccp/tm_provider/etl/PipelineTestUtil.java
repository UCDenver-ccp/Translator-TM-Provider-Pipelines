package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_COLLECTIONS;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;

import java.util.ArrayList;
import java.util.List;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;

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

	public static ProcessingStatus createProcessingStatus(String documentId, ProcessingStatusFlag... flagsToSet) {
		ProcessingStatus ps = new ProcessingStatus(documentId);
		for (ProcessingStatusFlag flag : flagsToSet) {
			ps.enableFlag(flag);
		}
		return ps;
	}

	public static Entity createEntity(String documentId, String... collectionNames) {
		Key key = DatastoreKeyUtil.createStatusKey(documentId);
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(STATUS_PROPERTY_DOCUMENT_ID, makeValue(documentId).build());
		List<Value> collections = new ArrayList<Value>();
		for (String name : collectionNames) {
			collections.add(makeValue(name).build());
		}
		entityBuilder.putProperties(STATUS_PROPERTY_COLLECTIONS, makeValue(collections).build());
		Entity entity1 = entityBuilder.build();
		return entity1;
	}

	public static ProcessingStatus createProcessingStatus(String documentId, String... collectionNames) {
		ProcessingStatus ps = new ProcessingStatus(documentId);
		for (String collectionName : collectionNames) {
			ps.addCollection(collectionName);
		}
		return ps;
	}
}
