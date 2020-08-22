package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.beam.sdk.values.KV;

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
	 * @param statusEntity
	 * @param documentCriteria
	 * @return a mapping from the status entity (just passing it through) to a
	 *         mapping from each document criteria to the document content extracted
	 *         from Datastore
	 */
	public KV<com.google.datastore.v1.Entity, Map<DocumentCriteria, String>> getStatusEntityToContent(
			com.google.datastore.v1.Entity statusEntity, List<DocumentCriteria> documentCriteria) {
		String documentId = statusEntity.getPropertiesMap().get(STATUS_PROPERTY_DOCUMENT_ID).getStringValue();

		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();

		for (DocumentCriteria docCriteria : documentCriteria) {
			// documents can be stored in chunks depending on their size. Query for each
			// chunk here.
			StringBuffer content = new StringBuffer();

			int chunkIndex = 0;
			Key key = createDocumentKey(documentId, docCriteria, chunkIndex++);
			Entity entity = datastore.get(key);

			while (entity != null) {
				content.append(getDocumentContent(entity));
				key = createDocumentKey(documentId, docCriteria, chunkIndex++);
				entity = datastore.get(key);
			}

			map.put(docCriteria, content.toString());

		}

		return KV.of(statusEntity, map);

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

		Entity statusEntity = datastore.get(createStatusKey(documentId));

//		for (int i = 0; i < documentCriteria.size(); i++) {
//			DocumentType documentType = documentCriteria.get(i).getDocumentType();
//			PipelineKey pipelineKey = documentCriteria.get(i).getPipelineKey();
//			DocumentFormat documentFormat = documentCriteria.get(i).getDocumentFormat();
//			String pipelineVersion = documentCriteria.get(i).getPipelineVersion();

		for (DocumentCriteria dc : documentCriteria) {

			// TODO: Workaround - there are max 7 chunks in the CORD data, so we try to grab
			// as many as there are
			// the chunkCount fields are not in the status entity for some reason
			// int chunkCount = getChunkCount(statusEntity, dc);
			int chunkCount = 7;
			// get each chunk and combine to create the document content
			StringBuffer content = new StringBuffer();
			for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
				try {
					Key key = createDocumentKey(documentId, dc, chunkIndex);
					Entity entity = datastore.get(key);
					content.append(getDocumentContent(entity));
				} catch (Exception e) {
					// TODO: this is part of the workaround - if an exception is thrown then we
					// assume that we have all of the chunks
					break;
				}
			}

			typeToContentMap.put(dc.getDocumentType(), content.toString());

		}

//		for (Iterator<Entity> entityIter = datastore.get(keys); entityIter.hasNext();) {
//			Entity entity = entityIter.next();
//			typeToContentMap.put(getDocumentType(entity), getDocumentContent(entity));
//		}

		return typeToContentMap;

	}

	private int getChunkCount(Entity status, DocumentCriteria dc) {
		return new Long(status.getLong(DatastoreProcessingStatusUtil.getDocumentChunkCountPropertyName(dc))).intValue();
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
	public Key createDocumentKey(String docId, DocumentCriteria docCriteria, int chunkIndex) {
		String docName = DatastoreKeyUtil.getDocumentKeyName(docId, docCriteria, chunkIndex);// type, format, pipeline,
		// pipelineVersion);
		return datastore.newKeyFactory()
				.addAncestor(PathElement.of(STATUS_KIND, DatastoreKeyUtil.getStatusKeyName(docId)))
				.setKind(DOCUMENT_KIND).newKey(docName);
	}

	public Key createStatusKey(String docId) {
		String keyName = DatastoreKeyUtil.getStatusKeyName(docId);
		return datastore.newKeyFactory().setKind(STATUS_KIND).newKey(keyName);
	}
}
