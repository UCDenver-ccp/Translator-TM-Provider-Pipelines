package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_YEAR_PUBLISHED;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_PUBLICATION_TYPES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Entity.Builder;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Transaction;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SuccessStatusFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ToKVFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.UpdateStatusFn;

/**
 * Utility for querying/updating 'Status' entities in Cloud Datastore.
 */
public class DatastoreProcessingStatusUtil {

	public enum OverwriteOutput {
		/**
		 * If yes, this flag indicates to
		 */
		YES, NO
	}

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	public static String getDocumentId(com.google.datastore.v1.Entity statusEntity) {
		return statusEntity.getPropertiesMap().get(STATUS_PROPERTY_DOCUMENT_ID).getStringValue();
	}

	public static String getYearPublished(com.google.datastore.v1.Entity statusEntity) {
		if (statusEntity.getPropertiesMap().containsKey(STATUS_PROPERTY_YEAR_PUBLISHED)) {
			return statusEntity.getPropertiesMap().get(STATUS_PROPERTY_YEAR_PUBLISHED).getStringValue();
		}
		return null;
	}

	public static List<String> getPublicationTypes(com.google.datastore.v1.Entity statusEntity) {
		if (statusEntity.getPropertiesMap().containsKey(STATUS_PROPERTY_PUBLICATION_TYPES)) {
			return Arrays.asList(statusEntity.getPropertiesMap().get(STATUS_PROPERTY_PUBLICATION_TYPES).getStringValue()
					.split("\\|"));
		}
		return Collections.emptyList();
	}

	/**
	 * @param dc
	 * @return the name of the chunk_count property (in a status entity) for the
	 *         specified document
	 */
	public static String getDocumentChunkCountPropertyName(DocumentCriteria dc) {
		return DatastoreKeyUtil.getDocumentKeyName("chunks", dc, 0);
	}

	/**
	 * @param targetProcessStatusFlag    The flag indicating the desired (target)
	 *                                   process to run
	 * @param requiredProcessStatusFlags Required flags prior to running the desired
	 *                                   (target) process
	 * @return
	 */
	public List<String> getDocumentIdsInNeedOfProcessing(ProcessingStatusFlag targetProcessStatusFlag,
			Set<ProcessingStatusFlag> requiredProcessStatusFlags, String collection, OverwriteOutput overwrite) {

		/*
		 * convert the required status flags into an array of PropertyFilters that match
		 * 'true'
		 */
		List<PropertyFilter> requiredProcessStatusFlagFilters = requiredProcessStatusFlags.stream()
				.map(flag -> PropertyFilter.eq(flag.getDatastoreFlagPropertyName(), true)).collect(Collectors.toList());

		/* add a filter on the collection name if one has been provided */
		if (collection != null) {
//			PropertyFilter collectionFilter = PropertyFilter.eq(collection.getDatastoreFlagPropertyName(), true);
			PropertyFilter collectionFilter = PropertyFilter.eq(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS,
					collection);
			requiredProcessStatusFlagFilters.add(collectionFilter);
		}

		/*
		 * require the target process to not have run, i.e. its flag == false AND the
		 * required process flags to all equal true
		 */
		CompositeFilter filter;
		if (overwrite == OverwriteOutput.NO) {
			filter = CompositeFilter.and(
					PropertyFilter.eq(targetProcessStatusFlag.getDatastoreFlagPropertyName(), false),
					requiredProcessStatusFlagFilters.toArray(new PropertyFilter[0]));
		} else {
			PropertyFilter first = requiredProcessStatusFlagFilters.get(0);
			requiredProcessStatusFlagFilters.remove(0);
			PropertyFilter[] rest = requiredProcessStatusFlagFilters.toArray(new PropertyFilter[0]);
			filter = CompositeFilter.and(first, rest);
		}

		// FIXME: Note that this query could probably be converted to a key-only query
		// and therefore be more cost-effective. The document Id can be parsed from each
		// key.
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(STATUS_KIND).setFilter(filter).build();

		QueryResults<Entity> results = datastore.run(query);

		/* convert the query results into a list of document IDs */
		List<String> documentIds = Streams.stream(results)
				.map(entity -> entity.getString(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID))
				.collect(Collectors.toList());

		return documentIds;
	}

	/**
	 * @param targetProcessStatusFlag
	 * @return a list of document IDs where the specified ProcessingStatusFlag ==
	 *         true
	 */
	public List<String> getDocumentIdsAlreadyProcessed(ProcessingStatusFlag targetProcessStatusFlag) {

		// FIXME: Note that this query could probably be converted to a key-only query
		// and therefore be more cost-effective. The document Id can be parsed from each
		// key.
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(STATUS_KIND)
				.setFilter(PropertyFilter.eq(targetProcessStatusFlag.getDatastoreFlagPropertyName(), true)).build();

		QueryResults<Entity> results = datastore.run(query);

		/* convert the query results into a list of document IDs */
		List<String> documentIds = Streams.stream(results)
				.map(entity -> entity.getString(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID))
				.collect(Collectors.toList());

		return documentIds;
	}

	/**
	 * Executes a transaction with the Datastore to set the specified status flags
	 * to true. All status flags for a given document need to be called prior to the
	 * update to avoid the following error:
	 * com.google.cloud.datastore.DatastoreException: too much contention on these
	 * datastore entities. please try again.
	 * 
	 * @param docId
	 * @param statusFlag
	 */
	public void setStatusTrue(String docId, Set<ProcessingStatusFlag> statusFlags) {
		String keyName = DatastoreKeyUtil.getStatusKeyName(docId);
		Key key = datastore.newKeyFactory().setKind(STATUS_KIND).newKey(keyName);

		Transaction transaction = datastore.newTransaction();
		try {
			Entity status = transaction.get(key);
			if (status != null) {
				Builder builder = Entity.newBuilder(status);
				statusFlags.remove(ProcessingStatusFlag.NOOP);
				for (ProcessingStatusFlag flag : statusFlags) {
					builder.set(flag.getDatastoreFlagPropertyName(), true);
				}
				transaction.put(builder.build());
			} else {
				throw new IllegalArgumentException(
						String.format("Unable to find status for key %s. Cannot update status for tasks:%s.", keyName,
								statusFlags.toString()));
			}
			transaction.commit();
		} finally {
			if (transaction.isActive()) {
				transaction.rollback();
			}
		}
	}

	public void setStatus(List<Key> keys, Set<ProcessingStatusFlag> statusFlags, boolean status) {
		Transaction transaction = datastore.newTransaction();
		try {

			int count = 0;
			for (Key key : keys) {
				if (count++ % 100 == 0) {
					System.out.println("progress: " + count);
				}
//				String keyName = DatastoreKeyUtil.getStatusKeyName(docId);
//				Key key = datastore.newKeyFactory().setKind(STATUS_KIND).newKey(keyName);

				Entity statusEntity = transaction.get(key);
				if (statusEntity != null) {
					Builder builder = Entity.newBuilder(statusEntity);
					statusFlags.remove(ProcessingStatusFlag.NOOP);
					for (ProcessingStatusFlag flag : statusFlags) {
						builder.set(flag.getDatastoreFlagPropertyName(), status);
					}
					transaction.put(builder.build());
				} else {
					throw new IllegalArgumentException(
							String.format("Unable to find status for key %s. Cannot update status for tasks:%s.",
									key.toString(), statusFlags.toString()));
				}
			}
			transaction.commit();
		} finally {
			if (transaction.isActive()) {
				transaction.rollback();
			}
		}
	}

	/**
	 * Logs the status of processing (set the flag=true) for the specified
	 * {@ProcessingStatusFlag} for documents that did not fail as true.
	 * 
	 * @param docIdToOutputDoc
	 * @param failures
	 * @return returns KV pairs mapping document ID to the
	 *         {@link ProcessingStatusFlag} for the document IDs that were
	 *         successfully processed.
	 */
	public static PCollection<KV<String, String>> getSuccessStatus(PCollection<String> processedDocIds,
			PCollection<EtlFailureData> failures, ProcessingStatusFlag processingStatusFlag) {
		PCollection<String> failureDocIds = failures.apply("extract failure doc IDs",
				ParDo.of(new DoFn<EtlFailureData, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element EtlFailureData failure, OutputReceiver<String> out) {
						out.output(failure.getDocumentId());
					}

				}));

//		PCollection<String> processedDocIds = docIdToConllu.apply(Keys.<String>create());
		// pair all document id with 'true' b/c we need a KV pair to combine them
		PCollection<KV<String, Boolean>> processedDocIdsKV = processedDocIds.apply("create all doc ids map",
				ParDo.of(new ToKVFn()));
		// pair the failed document id with 'true' b/c we need a KV pair to combine them
		PCollection<KV<String, Boolean>> failureDocIdsKV = failureDocIds.apply("create failed doc ids map",
				ParDo.of(new ToKVFn()));

		// Merge collection values into a CoGbkResult collection.
		final TupleTag<Boolean> allTag = new TupleTag<Boolean>();
		final TupleTag<Boolean> failedTag = new TupleTag<Boolean>();
		PCollection<KV<String, CoGbkResult>> joinedCollection = KeyedPCollectionTuple.of(allTag, processedDocIdsKV)
				.and(failedTag, failureDocIdsKV).apply(CoGroupByKey.<String>create());

		return joinedCollection.apply(ParDo.of(new SuccessStatusFn(processingStatusFlag, allTag, failedTag)));
	}

	/**
	 * Performs updates on the status of each document in batch, i.e. all status
	 * updates for a single document should be performed in a single transaction.
	 * 
	 * @param statusList
	 */
	public static void performStatusUpdatesInBatch(List<PCollection<KV<String, String>>> statusList) {
		// merge all docIdToStatusToUpdate KV's by key (document id), then perform the
		// update once per document ID
		List<TupleTag<String>> tags = new ArrayList<TupleTag<String>>();
		PCollection<KV<String, CoGbkResult>> mergedStatus = combineStatusLists(statusList, tags);

		mergedStatus.apply("update status", ParDo.of(new UpdateStatusFn(tags)));
	}

	static PCollection<KV<String, CoGbkResult>> combineStatusLists(List<PCollection<KV<String, String>>> statusList,
			List<TupleTag<String>> tags) {
		for (int i = 0; i < statusList.size(); i++) {
			tags.add(new TupleTag<String>());
		}

		KeyedPCollectionTuple<String> collectionTuple = KeyedPCollectionTuple.of(tags.get(0), statusList.get(0));
		for (int i = 1; i < statusList.size(); i++) {
			collectionTuple = collectionTuple.and(tags.get(i), statusList.get(i));
		}
		PCollection<KV<String, CoGbkResult>> mergedStatus = collectionTuple.apply("merge status by document",
				CoGroupByKey.<String>create());
		return mergedStatus;
	}

}
