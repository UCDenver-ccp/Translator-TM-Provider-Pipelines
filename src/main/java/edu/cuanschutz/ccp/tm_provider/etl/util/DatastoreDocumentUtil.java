package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;

/**
 * Utility for querying 'Documents' in Cloud Datastore.
 */
public class DatastoreDocumentUtil {

	private static Logger logger = Logger.getLogger(DatastoreDocumentUtil.class.getName());
	
	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	/**
	 * @param documentIds
	 * @param type
	 * @param format
	 * @return a list of document ID/content pairs queried from Cloud Datastore
	 */
	public List<KV<String, String>> getDocumentIdToContentList(List<String> documentIds, DocumentType type,
			DocumentFormat format, PipelineKey pipeline, String pipelineVersion, int maxNumToProcess) {

		// from the input list of document IDs, create an array of corresponding
		// document @{link Key} objects
		Key[] keys = documentIds.stream().map(id -> createDocumentKey(id, type, format, pipeline, pipelineVersion))
				.collect(Collectors.toList()).toArray(new Key[0]);

		// can't request more than 1000 keys - so limit to 999 here
		Key[] keysToUse = keys;
		if (keys.length > maxNumToProcess) {
			keysToUse = Arrays.copyOfRange(keys, 0, maxNumToProcess);
		}

		// Batch query for the entities corresponding to the {@link Key} objects, and
		// populate a list of document id/content KV pairs
		List<KV<String, String>> docIdToContentPairs = Streams.stream(datastore.get(keysToUse))
				.map(entity -> KV.of(getDocumentId(entity), getDocumentContent(entity))).collect(Collectors.toList());
		return docIdToContentPairs;
	}

	public KV<String, String> getDocumentIdToContent(String documentId, DocumentType type, DocumentFormat format,
			PipelineKey pipeline, String pipelineVersion) {

		// from the input list of document IDs, create an array of corresponding
		// document @{link Key} objects
		Key key = createDocumentKey(documentId, type, format, pipeline, pipelineVersion);
		Entity entity = datastore.get(key);

		if( entity==null) {
			logger.log(Level.WARNING, "No text document for documentID: " + documentId);
			return null;
		}
		String id = getDocumentId(entity);
		String content = getDocumentContent(entity);
		
		return KV.of(id, content);
	}

	/**
	 * @param documentId
	 * @param documentTypes
	 * @param documentFormats
	 * @param pipelines
	 * @param pipelineVersions
	 * @return for the given document ID, return a mapping from {@link DocumentType}
	 *         to the document content for all types specified
	 */
	public Map<DocumentType, String> getDocumentTypeToContent(String documentId,
			List<DocumentCriteria> documentCriteria) {

		Map<DocumentType, String> typeToContentMap = new HashMap<DocumentType, String>();

		Key[] keys = new Key[documentCriteria.size()];
		for (int i = 0; i < documentCriteria.size(); i++) {
			Key key = createDocumentKey(documentId, documentCriteria.get(i).getDocumentType(),
					documentCriteria.get(i).getDocumentFormat(), documentCriteria.get(i).getPipelineKey(),
					documentCriteria.get(i).getPipelineVersion());
			try {
			Entity entity = datastore.get(key);
			typeToContentMap.put(getDocumentType(entity), getDocumentContent(entity));
			} catch (Throwable t) {
				// will throw error if doc doesn't exists, and it might not exist yet
				// do nothing
			}
			
		}
		
		
		
//		for (Iterator<Entity> entityIter = datastore.get(keys); entityIter.hasNext();) {
//			Entity entity = entityIter.next();
//			typeToContentMap.put(getDocumentType(entity), getDocumentContent(entity));
//		}

		return typeToContentMap;

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
	 * @param document
	 * @return the document type for the given document {@link Entity}
	 */
	private DocumentType getDocumentType(Entity document) {
		return DocumentType.valueOf(document.getString(DOCUMENT_PROPERTY_TYPE));
	}

	/**
	 * @param docId
	 * @param type
	 * @param format
	 * @return the Key for the specified document
	 */
	public Key createDocumentKey(String docId, DocumentType type, DocumentFormat format, PipelineKey pipeline,
			String pipelineVersion) {
		String docName = DatastoreKeyUtil.getDocumentKeyName(docId, type, format, pipeline, pipelineVersion);
		return datastore.newKeyFactory()
				.addAncestor(PathElement.of(STATUS_KIND, DatastoreKeyUtil.getStatusKeyName(docId)))
				.setKind(DOCUMENT_KIND).newKey(docName);
	}
}
