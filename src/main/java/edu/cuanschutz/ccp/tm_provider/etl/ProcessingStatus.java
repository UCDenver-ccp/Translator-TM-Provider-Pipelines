package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A data structure summarizing the processing status for a given document
 */
@ToString
@SuppressWarnings("rawtypes")
@EqualsAndHashCode(callSuper = false)
public class ProcessingStatus extends DoFn {

	private static final long serialVersionUID = 1L;

	@Getter
	private final String documentId;

	@Getter
	@Setter
	private String yearPublished = "9999";

	@Getter
	private List<String> publicationTypes = new ArrayList<String>();

	@Getter
	private Map<String, Boolean> flagPropertiesMap;

	@Getter
	private Set<String> collections;

	public ProcessingStatus(String documentId) {
		this.documentId = documentId;
		this.flagPropertiesMap = new HashMap<String, Boolean>();
	}

	public ProcessingStatus(Entity statusEntity) {
		this.documentId = DatastoreProcessingStatusUtil.getDocumentId(statusEntity);
		this.yearPublished = DatastoreProcessingStatusUtil.getYearPublished(statusEntity);
		for (String pubType : DatastoreProcessingStatusUtil.getPublicationTypes(statusEntity)) {
			this.publicationTypes.add(pubType);
		}
		this.flagPropertiesMap = new HashMap<String, Boolean>();
		for (Entry<String, Value> entry : statusEntity.getPropertiesMap().entrySet()) {
			if (entry.getKey().equals(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS)) {
				List<Value> collectionNames = DatastoreHelper.getList(entry.getValue());
				for (Value v : collectionNames) {
					addCollection(v.getStringValue());
				}
			} else if (entry.getKey().equals(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID)) {
				// do nothing -- the documentId field has already been assigned
//				this.documentId = entry.getValue().getStringValue();
			} else {
				flagPropertiesMap.put(entry.getKey(), entry.getValue().getBooleanValue());
			}
		}
	}

	public ProcessingStatus(ProcessingStatus statusEntity) {
		this.documentId = statusEntity.getDocumentId();
		this.yearPublished = statusEntity.getYearPublished();
		for (String pubType : statusEntity.getPublicationTypes()) {
			this.publicationTypes.add(pubType);
		}
		if (statusEntity.getCollections() != null) {
			for (String collection : statusEntity.getCollections()) {
				addCollection(collection);
			}
		}
		this.flagPropertiesMap = new HashMap<String, Boolean>();
		for (Entry<String, Boolean> entry : statusEntity.getFlagPropertiesMap().entrySet()) {
			setFlagProperty(entry.getKey(), entry.getValue());
		}
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

	public Set<String> getFlagProperties() {
		return new HashSet<String>(flagPropertiesMap.keySet());
	}

	public void setFlagProperty(String property, boolean value) {
		this.flagPropertiesMap.put(property, value);
	}

	/**
	 * Sets the corresponding status flag to true
	 * 
	 * @param flag
	 */
	public void enableFlag(ProcessingStatusFlag flag) {// , int correspondingChunkCount) {
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
		if (flag != ProcessingStatusFlag.NOOP) {
			String flagPropertyName = flag.getDatastoreFlagPropertyName();
			flagPropertiesMap.put(flagPropertyName, status);
		}
	}

	public void addPublicationType(String pubType) {
		this.publicationTypes.add(pubType);
	}

}
