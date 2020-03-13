package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;

/**
 * Utility for querying 'Documents' in Cloud Datastore.
 */
public class DatastoreDocumentUtil {

	private final static Logger LOGGER = Logger.getLogger(DatastoreDocumentUtil.class.getName());

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	// Create a Key factory to construct keys associated with this project.
	private final KeyFactory keyFactory = datastore.newKeyFactory().setKind(DOCUMENT_KIND);

	/**
	 * @param documentIds
	 * @param type
	 * @param format
	 * @return a list of document ID/content pairs queried from Cloud Datastore
	 */
	public List<KV<String, String>> getDocumentIdToContentList(List<String> documentIds, DocumentType type,
			DocumentFormat format, PipelineKey pipeline, String pipelineVersion) {

		// from the input list of document IDs, create an array of corresponding
		// document @{link Key} objects
		Key[] keys = documentIds.stream().map(id -> createDocumentKey(id, type, format, pipeline, pipelineVersion))
				.collect(Collectors.toList()).toArray(new Key[0]);

		LOGGER.log(Level.INFO, String.format("key count: %d -- %s", keys.length, Arrays.toString(keys)));

		// Batch query for the entities corresponding to the {@link Key} objects, and
		// populate a list of document id/content KV pairs
		List<KV<String, String>> docIdToContentPairs = Streams.stream(datastore.get(keys))
				.map(entity -> KV.of(getDocumentId(entity), getDocumentContent(entity))).collect(Collectors.toList());
		return docIdToContentPairs;
	}

	/**
	 * @param document
	 * @return the document content for the given document {@link Entity}
	 */
	private String getDocumentContent(Entity document) {
		return new String(document.getBlob(DOCUMENT_PROPERTY_CONTENT).toByteArray());
	}

	/**
	 * @param document
	 * @return the document ID for the given document {@link Entity}
	 */
	private String getDocumentId(Entity document) {
		return document.getString(DOCUMENT_PROPERTY_ID);
	}

	/**
	 * @param docId
	 * @param type
	 * @param format
	 * @return the Key for the specified document
	 */
	public Key createDocumentKey(String docId, DocumentType type, DocumentFormat format, PipelineKey pipeline,
			String pipelineVersion) {
		String docName = getDocumentKeyName(docId, type, format, pipeline, pipelineVersion);
		return keyFactory.newKey(docName);
	}

	public static String getDocumentKeyName(String docId, DocumentType type, DocumentFormat format,
			PipelineKey pipeline, String version) {
		return String.format("%s.%s.%s.%s.%s", docId, type.name().toLowerCase(), format.name().toLowerCase(),
				pipeline.name().toLowerCase(), version);
	}

}
