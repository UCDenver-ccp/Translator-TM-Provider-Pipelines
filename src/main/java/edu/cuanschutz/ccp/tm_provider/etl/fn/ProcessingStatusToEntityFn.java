package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_COLLECTIONS;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_PUBLICATION_TYPES;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_YEAR_PUBLISHED;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.SerializableFunction;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Transforms a PCollection containing {@lnk ProcessingStatus} objects to a
 * PCollection containing Google Cloud Datastore Entities
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ProcessingStatusToEntityFn extends DoFn<ProcessingStatus, KV<String, Entity>> {

	private static final long serialVersionUID = 1L;

	private final SerializableFunction<String, String> collectionFn;

	public ProcessingStatusToEntityFn(SerializableFunction<String, String> collectionFn) {
		this.collectionFn = collectionFn;
	}

	public ProcessingStatusToEntityFn() {
		this.collectionFn = null;
	}

	@ProcessElement
	public void processElement(@Element ProcessingStatus status, OutputReceiver<KV<String, Entity>> out)
			throws UnsupportedEncodingException {
		if (collectionFn != null) {
			for (String c : ToEntityFnUtils.getCollections(null, collectionFn, status.getDocumentId())) {
				status.addCollection(c);
			}
		}
		Entity entity = buildStatusEntity(status);
		out.output(KV.of(entity.getKey().toString(), entity));

	}

	public static Entity buildStatusEntity(ProcessingStatus status) {
		com.google.datastore.v1.Key key = DatastoreKeyUtil.createStatusKey(status.getDocumentId());

		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(STATUS_PROPERTY_DOCUMENT_ID, makeValue(status.getDocumentId()).build());

		entityBuilder.putProperties(STATUS_PROPERTY_YEAR_PUBLISHED, makeValue(status.getYearPublished()).build());
		String serializedPubTypes = serializePublicationTypes(status.getPublicationTypes());
		entityBuilder.putProperties(STATUS_PROPERTY_PUBLICATION_TYPES, makeValue(serializedPubTypes).build());

		Set<String> setFlagProperties = new HashSet<String>();
		for (String flagProperty : status.getFlagProperties()) {
			entityBuilder.putProperties(flagProperty, makeValue(status.getFlagPropertyValue(flagProperty)).build());
			setFlagProperties.add(flagProperty);
		}

		// for any flag that hasn't been set, set it to false
		for (ProcessingStatusFlag flag : ProcessingStatusFlag.values()) {
			if (flag != ProcessingStatusFlag.NOOP) {
				String propertyName = flag.getDatastoreFlagPropertyName();
				if (!setFlagProperties.contains(propertyName)) {
					entityBuilder.putProperties(propertyName, makeValue(false).build());
				}
			}
		}

//		for (String countProperty : status.getCountProperties()) {
//			entityBuilder.putProperties(countProperty, makeValue(status.getCountPropertyValue(countProperty)).build());
//		}

		// store document collection indicators
		Set<Value> collectionValues = new HashSet<Value>();
		if (status.getCollections() != null) {
			for (String collection : status.getCollections()) {
				collectionValues.add(makeValue(collection).build());
			}
		}
		entityBuilder.putProperties(STATUS_PROPERTY_COLLECTIONS, makeValue(collectionValues).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

	private static String serializePublicationTypes(List<String> publicationTypes) {
		return CollectionsUtil.createDelimitedString(publicationTypes, "|");
	}

	public static List<String> deserializePublicationTypes(String serializedPubTypes) {
		return Arrays.asList(serializedPubTypes.split("\\|"));
	}

	static ProcessingStatus getStatus(Entity entity) {

		Map<String, Value> propertiesMap = entity.getPropertiesMap();
		String documentId = propertiesMap.get(STATUS_PROPERTY_DOCUMENT_ID).getStringValue();
		ProcessingStatus status = new ProcessingStatus(documentId);

		String yearPublished = propertiesMap.get(STATUS_PROPERTY_YEAR_PUBLISHED).getStringValue();
		status.setYearPublished(yearPublished);

		String serializedPubTypes = propertiesMap.get(STATUS_PROPERTY_PUBLICATION_TYPES).getStringValue();
		for (String pubType : deserializePublicationTypes(serializedPubTypes)) {
			status.addPublicationType(pubType);
		}

		propertiesMap.remove(STATUS_PROPERTY_DOCUMENT_ID);
		propertiesMap.remove(STATUS_PROPERTY_YEAR_PUBLISHED);
		propertiesMap.remove(STATUS_PROPERTY_PUBLICATION_TYPES);

		for (Entry<String, Value> entry : propertiesMap.entrySet()) {
			String propertyName = entry.getKey();
			if (propertyName.endsWith("_DONE")) {
				boolean booleanValue = entry.getValue().getBooleanValue();
				status.setFlagProperty(propertyName, booleanValue);
			} else if (propertyName.endsWith("_COUNT")) {
//				long countValue = entry.getValue().getIntegerValue();
//				status.setCountProperty(propertyName, countValue);
			} else if (propertyName.equals(STATUS_PROPERTY_COLLECTIONS)) {
				List<Value> valuesList = entry.getValue().getArrayValue().getValuesList();
				for (Value v : valuesList) {
					status.addCollection(v.getStringValue());
				}
			} else {
				throw new IllegalArgumentException(String.format(
						"Encountered unexptected status property: %s. Unable to create ProcessingStatus from Datastore entity.",
						propertyName));
			}
		}

		return status;
	}

}
