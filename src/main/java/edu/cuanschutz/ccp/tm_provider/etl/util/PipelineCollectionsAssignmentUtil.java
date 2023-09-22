package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_FORMAT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_COLLECTIONS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

/**
 * NOTE - this class works, but is WAAAYYY TOO SLOW to be of use.
 * 
 * 
 * This class was originally designed to query datastore and assign a new
 * collection to records that had failed processing during a previous run (of
 * some kind). For example, we have some failures during the CRF pipeline. This
 * code will allow us to query for the documents that failed and assign them a
 * unique collection so that they can be re-processed in bulk.
 *
 */
public class PipelineCollectionsAssignmentUtil {

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	/**
	 * @param docCriteria                - describes the documents that we expect to
	 *                                   see - we will be looking for document IDs
	 *                                   that are missing these documents
	 * @param collection                 - the input collection - used in the
	 *                                   initial search for the expected documents
	 * @param newCollection              - the collection that will be assigned to
	 *                                   any status entity that is missing the
	 *                                   document of interest
	 * @param targetProcessingStatusFlag - the relevant processing status flag will
	 *                                   be set to false in the status entity
	 */
	public void assignCollectionBasedOnMissingDocuments(DocumentCriteria docCriteria, String collection,
			String newCollection, ProcessingStatusFlag targetProcessingStatusFlag) {

		/* retrieve document IDs when the document of interest is observed */
		Set<String> docIdsWithDocumentOfInterest = getDocumentKeys(docCriteria, collection);
		System.out.println(String.format("Existing document count: %d", docIdsWithDocumentOfInterest.size()));

		/*
		 * retrieve a mapping from document ID to status key for all status entities
		 * belonging to the collection of interest
		 */
		Map<String, Key> documentIdToStatusKey = getDocumentIdToStatusKey(collection);
		System.out.println(String.format("Existing status count: %d", documentIdToStatusKey.size()));

		/* set difference to find document IDs missing the document of interest */
		Set<String> docIdsMissingDocOfInterest = new HashSet<String>(documentIdToStatusKey.keySet());
		docIdsMissingDocOfInterest.removeAll(docIdsWithDocumentOfInterest);
		System.out.println(String.format("Missing document count: %d", docIdsMissingDocOfInterest.size()));

		/*
		 * update the status entities that are missing the document of interest by
		 * adding the new-collection, and setting the target processing status flag to
		 * false. Transactions happen in batches.
		 */
		HashSet<ProcessingStatusFlag> flagsToUpdate = new HashSet<ProcessingStatusFlag>(
				Arrays.asList(targetProcessingStatusFlag));
		boolean flagUpdateStatus = false;
		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();
		int count = 0;
		List<Key> keys = new ArrayList<Key>();
		for (String docId : docIdsMissingDocOfInterest) {
			if (count++ % 250 == 0) {
				System.out.println(String.format("status update progress: %d of %d", (count - 1),
						docIdsMissingDocOfInterest.size()));
				util.setStatusAndAddCollection(keys, flagsToUpdate, flagUpdateStatus, newCollection);
				keys = new ArrayList<Key>();
			}
			keys.add(documentIdToStatusKey.get(docId));
		}

		/* set the final group */
		util.setStatusAndAddCollection(keys, flagsToUpdate, flagUpdateStatus, newCollection);

	}

	/**
	 * Query datastore for documents that match the specified
	 * {@link DocumentCriteria} and are in the specified collection. Return a set of
	 * the associated document identifies.
	 * 
	 * @param docCriteria
	 * @param collection
	 * @return
	 */
	private Set<String> getDocumentKeys(DocumentCriteria docCriteria, String collection) {
		KeyQuery.Builder q = KeyQuery.newKeyQueryBuilder();
		q.setKind(DOCUMENT_KIND);

		CompositeFilter filter = com.google.cloud.datastore.StructuredQuery.CompositeFilter.and(
				com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(DOCUMENT_PROPERTY_TYPE,
						docCriteria.getDocumentType().name()),
				com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(DOCUMENT_PROPERTY_FORMAT,
						docCriteria.getDocumentFormat().name()),
				com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(DOCUMENT_PROPERTY_PIPELINE,
						docCriteria.getPipelineKey().name()),
				com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(DOCUMENT_PROPERTY_PIPELINE_VERSION,
						docCriteria.getPipelineVersion()),
				com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(DOCUMENT_PROPERTY_COLLECTIONS,
						collection));

		q.setFilter(filter);
		QueryResults<Key> results = datastore.run(q.build());
		int count = 0;
		Set<String> docIdsWithDocumentOfInterest = new HashSet<String>();
		while (results.hasNext()) {
			if (count++ % 1000 == 0) {
				System.out.println("query for doc IDs progress: " + count);
			}
			Key key = results.next();

			String docId = key.getName().substring(0, key.getName().indexOf("."));
			docIdsWithDocumentOfInterest.add(docId);
		}
		return docIdsWithDocumentOfInterest;
	}

	/**
	 * Return a mapping from document ID to status entity datastore key for entities
	 * in the specified collection.
	 * 
	 * @param collection
	 * @return
	 */
	private Map<String, Key> getDocumentIdToStatusKey(String collection) {

		Map<String, Key> documentIdToStatusKey = new HashMap<String, Key>();
		KeyQuery.Builder statusKeyQ = KeyQuery.newKeyQueryBuilder();
		statusKeyQ.setKind(STATUS_KIND);
		statusKeyQ.setFilter(PropertyFilter.eq(STATUS_PROPERTY_COLLECTIONS, collection));
		QueryResults<Key> results = datastore.run(statusKeyQ.build());
		int count = 0;
		while (results.hasNext()) {
			if (count++ % 10000 == 0) {
				System.out.println("status id/key progress: " + count);
			}
			Key key = results.next();

			String docId = key.getName().substring(0, key.getName().indexOf("."));
			documentIdToStatusKey.put(docId, key);
		}
		return documentIdToStatusKey;
	}

	public static void main(String[] args) {

		DocumentType documentType = DocumentType.CRF_CRAFT;
		DocumentFormat documentFormat = DocumentFormat.BIONLP;
		PipelineKey pipelineKey = PipelineKey.CRF;
		String pipelineVersion = "0.3.0";

		DocumentCriteria docCriteria = new DocumentCriteria(documentType, documentFormat, pipelineKey, pipelineVersion);
		String collection = "PUBMED";
		String newCollection = "CRF_REDO_20230914";
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.CRF_DONE;
		new PipelineCollectionsAssignmentUtil().assignCollectionBasedOnMissingDocuments(docCriteria, collection,
				newCollection, targetProcessingStatusFlag);

	}

}
