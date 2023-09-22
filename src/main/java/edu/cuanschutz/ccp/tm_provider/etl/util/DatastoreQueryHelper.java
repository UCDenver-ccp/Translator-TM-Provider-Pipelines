package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery.Builder;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Value;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

public class DatastoreQueryHelper {

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	public void updateDependencyStatus() {
		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.DOCUMENT_KIND).build();

		Set<String> idsToUpdate = new HashSet<String>();
		QueryResults<Key> results = datastore.run(query);
		int count = 0;
		while (results.hasNext()) {
			if (count++ % 10000 == 0) {
				System.out.println("progress: " + count);
			}
			Key key = results.next();
			if (key.getName().contains("dependency")) {
				Pattern p = Pattern.compile("(PMC\\d+)");
				Matcher m = p.matcher(key.getName());
				if (m.find()) {
					idsToUpdate.add(m.group(1));
				} else {
					System.out.println("Can't parse id from: " + key.getName());
				}
			}
		}

		System.out.println("IDs to update DP_DONE=true: " + idsToUpdate.size());

		Set<ProcessingStatusFlag> flags = CollectionsUtil.createSet(ProcessingStatusFlag.DP_DONE);
		boolean value = true;

		setStatusFlags(idsToUpdate, flags, value);
	}

	public void printDocumentCountsBasedOnKeys() {
		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.DOCUMENT_KIND).build();

		Map<String, Integer> docTypeToCountMap = new HashMap<String, Integer>();
		Map<String, Set<String>> docIdToDoneMap = new HashMap<String, Set<String>>();

		QueryResults<Key> results = datastore.run(query);
		int count = 0;
		while (results.hasNext()) {
			if (count++ % 10000 == 0) {
				System.out.println("progress: " + count);
			}
			Key key = results.next();
			String keyName = key.getName();
			String docType = keyName.substring(keyName.indexOf("."));
			String docId = keyName.substring(0, keyName.indexOf("."));

			CollectionsUtil.addToOne2ManyUniqueMap(docId, docType, docIdToDoneMap);

			CollectionsUtil.addToCountMap(docType, docTypeToCountMap);
		}

		Map<String, Integer> doneToCountMap = new HashMap<String, Integer>();
		for (Entry<String, Set<String>> entry : docIdToDoneMap.entrySet()) {
			List<String> doneProcesses = new ArrayList<String>(entry.getValue());
			doneProcesses.remove(".bioc.biocxml.orig.na");
			Collections.sort(doneProcesses);

			CollectionsUtil.addToCountMap(doneProcesses.toString(), doneToCountMap);
		}

		Map<String, Integer> sortedMap = CollectionsUtil.sortMapByKeys(docTypeToCountMap, SortOrder.ASCENDING);
		sortedMap.entrySet().forEach(e -> System.out.println(e.getKey() + "  -- " + e.getValue()));

		System.out.println("----");
		sortedMap = CollectionsUtil.sortMapByKeys(doneToCountMap, SortOrder.ASCENDING);
		sortedMap.entrySet().forEach(e -> System.out.println(e.getKey() + "  -- " + e.getValue()));

	}

	public void getStatuses() {
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(DatastoreConstants.STATUS_KIND).build();

		QueryResults<Entity> results = datastore.run(query);
		int count = 0;
		int total = 0;
		while (results.hasNext()) {
			if (count++ % 100 == 0) {
				System.out.println("progress: " + count);
			}
			Entity entity = results.next();
			Map<String, Value<?>> properties = entity.getProperties();

//			 Boolean loadDone = Boolean.valueOf(properties.get(DatastoreConstants.STATUS_PROPERTY_BIGQUERY_LOAD_FILE_EXPORT_DONE).get().toString());
			Boolean chebiDone = Boolean
					.valueOf(properties.get(DatastoreConstants.STATUS_PROPERTY_OGER_CHEBI_DONE).get().toString());
//			 Boolean clDone = Boolean.valueOf(properties.get(DatastoreConstants.STATUS_PROPERTY_OGER_CL_DONE).get().toString());

			if (chebiDone) {
				total++;
			}

		}

		System.out.println("COUNT: " + count);
		System.out.println("TOTAL: " + total);

	}

	public void getDocumentKeyCount() {
		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.DOCUMENT_KIND).build();

		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();

		QueryResults<Key> results = datastore.run(query);
		int count = 0;
		while (results.hasNext()) {
			if (count++ % 10000 == 0) {
				System.out.println("progress: " + count);
			}
			Key key = results.next();
			if (key.getName().contains("chebi")) {
				Pattern p = Pattern.compile("(PMC\\d+)");
				Matcher m = p.matcher(key.getName());
				if (m.find()) {
//					System.out.println("setting true for: " +key.getName());
					util.setStatusTrue(m.group(1), CollectionsUtil.createSet(ProcessingStatusFlag.DP_DONE));
				} else {
					System.out.println("Can't parse id from: " + key.getName());
				}
			}
		}

	}

//	public void addDoidAndHgncStatusFields() {
//		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.STATUS_KIND).build();
//		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();
//
//		QueryResults<Key> results = datastore.run(query);
//		int count = 0;
//		List<Key> keys = new ArrayList<Key>();
//		while (results.hasNext()) {
//			if (count++ % 250 == 0) {
//				System.out.println("progress: " + count);
//				util.setStatus(keys, CollectionsUtil.createSet(ProcessingStatusFlag.OGER_DOID_DONE,
//						ProcessingStatusFlag.OGER_HGNC_DONE), false);
//				keys = new ArrayList<Key>();
//			}
//			Key key = results.next();
//			keys.add(key);
//		}
//
//		System.out.println("final setting flags...");
//		util.setStatus(keys,
//				CollectionsUtil.createSet(ProcessingStatusFlag.OGER_DOID_DONE, ProcessingStatusFlag.OGER_HGNC_DONE),
//				false);
//		System.out.println("done.");
//	}

	public void resetBigQueryFileGenStatusForAllDocuments() {
		// get statuses where the bigquery file has already been created

		com.google.cloud.datastore.KeyQuery.Builder query = Query.newKeyQueryBuilder()
				.setKind(DatastoreConstants.STATUS_KIND);
		query.setFilter(PropertyFilter
				.eq(ProcessingStatusFlag.BIGQUERY_LOAD_FILE_EXPORT_DONE.getDatastoreFlagPropertyName(), true));
		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();
		QueryResults<Key> results = datastore.run(query.build());
		int count = 0;
		List<Key> keys = new ArrayList<Key>();
		while (results.hasNext()) {
			if (count++ % 250 == 0) {
				System.out.println("progress: " + count);
				util.setStatus(keys, CollectionsUtil.createSet(ProcessingStatusFlag.BIGQUERY_LOAD_FILE_EXPORT_DONE),
						false);
				keys = new ArrayList<Key>();
			}
			Key key = results.next();
			keys.add(key);
		}

		System.out.println("final setting flags...");
		util.setStatus(keys, CollectionsUtil.createSet(ProcessingStatusFlag.BIGQUERY_LOAD_FILE_EXPORT_DONE), false);
		System.out.println("done.");
	}

//	/**
//	 * get the list of documetn ids to update from the directory that stores the
//	 * bioc xml files
//	 * 
//	 * @param dir
//	 * @throws IOException
//	 */
//	public void addDTestStatusFieldToAstmaDocs(File pmcIdListFile) throws IOException {
//
//		Set<String> docIdsToUpdate = new HashSet<String>();
//		for (StreamLineIterator lineIter = new StreamLineIterator(pmcIdListFile, CharacterEncoding.UTF_8); lineIter
//				.hasNext();) {
//			String line = lineIter.next().getText();
//			String docId = StringUtil.removeSuffix(line, ".xml");
//			docIdsToUpdate.add(docId);
//		}
//
//		Set<ProcessingStatusFlag> flags = CollectionsUtil.createSet(ProcessingStatusFlag.TEST);
//		boolean value = true;
//
//		setStatusFlags(docIdsToUpdate, flags, value);
//	}

	private void setStatusFlags(Set<String> docIdsToUpdate, Set<ProcessingStatusFlag> flags, boolean value) {
		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.STATUS_KIND).build();
		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();

		QueryResults<Key> results = datastore.run(query);
		int count = 0;
		List<Key> keys = new ArrayList<Key>();
		while (results.hasNext()) {
			if (count++ % 250 == 0) {
				System.out.println("progress: " + count);
				util.setStatus(keys, flags, value);
				keys = new ArrayList<Key>();
			}
			Key key = results.next();

			String docId = key.getName().substring(0, key.getName().indexOf("."));
			if (docIdsToUpdate.contains(docId)) {
				keys.add(key);
			}
		}

		System.out.println("final setting flags...");
		util.setStatus(keys, flags, value);
		System.out.println("done.");
	}

//	public void queryForStatusByCollection() {
//		// get statuses where the bigquery file has already been created
//
//		com.google.cloud.datastore.KeyQuery.Builder query = Query.newKeyQueryBuilder()
//				.setKind(DatastoreConstants.STATUS_KIND);
//		CompositeFilter filter = CompositeFilter.and(
//				PropertyFilter.eq(DatastoreConstants.STATUS_PROPERTY_IN_CORD19_COLLECTION, true),
//				PropertyFilter.eq(ProcessingStatusFlag.TEXT_DONE.getDatastoreFlagPropertyName(), true),
//				PropertyFilter.eq(ProcessingStatusFlag.OGER_CHEBI_DONE.getDatastoreFlagPropertyName(), false)
//				);
//		query.setFilter(filter);
//		QueryResults<Key> results = datastore.run(query.build());
//		while (results.hasNext()) {
//			Key key = results.next();
//			System.out.println(key.toString());
//		}
//	}

	public void collectionStatus(String collection) {
		// get statuses where the bigquery file has already been created

		Builder query = Query.newEntityQueryBuilder().setKind(DatastoreConstants.STATUS_KIND);
		query.setFilter(PropertyFilter.eq(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS, collection));
		QueryResults<Entity> results = datastore.run(query.build());
		int count = 0;

		Map<ProcessingStatusFlag, Integer> statusFlagToCountMap = new HashMap<ProcessingStatusFlag, Integer>();

		while (results.hasNext()) {
			if (count++ % 250 == 0) {
				System.out.println("progress: " + count);
			}
			Entity entity = results.next();
			Map<String, Value<?>> properties = entity.getProperties();

			for (ProcessingStatusFlag flag : ProcessingStatusFlag.values()) {
				if (properties.containsKey(flag.getDatastoreFlagPropertyName())) {
					Boolean done = Boolean
							.valueOf(properties.get(flag.getDatastoreFlagPropertyName()).get().toString());
					if (done) {
						CollectionsUtil.addToCountMap(flag, statusFlagToCountMap);
					}
				}
			}
		}

		System.out.println(String.format("%s document count: %d", collection, count));
		Map<ProcessingStatusFlag, Integer> sortedMap = CollectionsUtil.sortMapByValues(statusFlagToCountMap,
				SortOrder.DESCENDING);
		sortedMap.entrySet().forEach(e -> System.out.println(e.getKey() + " -- " + e.getValue()));
	}

	/**
	 * Add status flag(s) for every status entity in Datastore
	 * 
	 * @param flags
	 * @param initialValue
	 */
	public void addStatusFlag(Set<ProcessingStatusFlag> flags, boolean initialValue) {
		Query<Key> query = Query.newKeyQueryBuilder().setKind(DatastoreConstants.STATUS_KIND).build();
		DatastoreProcessingStatusUtil util = new DatastoreProcessingStatusUtil();

		QueryResults<Key> results = datastore.run(query);
		int count = 0;
		List<Key> keys = new ArrayList<Key>();
		while (results.hasNext()) {
			if (count++ % 250 == 0) {
				System.out.println("progress: " + count);
				util.setStatus(keys, flags, initialValue);
				keys = new ArrayList<Key>();
			}
			Key key = results.next();
			keys.add(key);
		}

		System.out.println("final setting flags...");
		util.setStatus(keys, flags, initialValue);
		System.out.println("done.");
	}

	public void getAllDocumentIds(File outputFile) throws IOException {
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(DatastoreConstants.STATUS_KIND).build();

		QueryResults<Entity> results = datastore.run(query);
		int count = 0;

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			while (results.hasNext()) {
				if (count++ % 1000 == 0) {
					System.out.println("progress: " + count);
				}
				Entity entity = results.next();
				Map<String, Value<?>> properties = entity.getProperties();

				String docId = properties.get(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID).get().toString();

				Boolean hasText = Boolean
						.valueOf(properties.get(DatastoreConstants.STATUS_PROPERTY_TEXT_DONE).get().toString());

				if (hasText) {
					writer.write(docId + "\n");
				}

			}
		}

	}

	public static void main(String[] args) throws IOException {
//		new DatastoreQueryHelper().getDocumentKeys();
//		new DatastoreQueryHelper().getStatuses();
//		new DatastoreQueryHelper().addDoidAndHgncStatusFields();

//		new DatastoreQueryHelper().resetBigQueryFileGenStatusForAllDocuments();

//		File pmcIdListFile = new File(
//				"/Users/bill/projects/ncats-translator/prototype/asthma-documents-to-add/asthma-files.list");
//
//		new DatastoreQueryHelper().addDTestStatusFieldToAstmaDocs(pmcIdListFile);

//		new DatastoreQueryHelper().updateDependencyStatus();
//		new DatastoreQueryHelper().queryForStatusByCollection();

//		new DatastoreQueryHelper().printDocumentCountsBasedOnKeys();

//		new DatastoreQueryHelper().addStatusFlag(CollectionsUtil.createSet(ProcessingStatusFlag.SENTENCE_DONE), false);

//		new DatastoreQueryHelper().collectionStatus("CORD19");

		File outputFile = new File("/Users/bill/projects/ncats-translator/current-kgs/20221205_document_ids.txt");
		new DatastoreQueryHelper().getAllDocumentIds(outputFile);

	}

}
