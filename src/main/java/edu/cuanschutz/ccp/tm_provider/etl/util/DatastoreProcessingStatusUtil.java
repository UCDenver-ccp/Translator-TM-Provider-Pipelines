package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

/**
 * Utility for querying/updating 'Status' entities in Cloud Datastore.
 */
public class DatastoreProcessingStatusUtil {

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	// Create a Key factory to construct keys associated with this project.
	private final KeyFactory keyFactory = datastore.newKeyFactory().setKind(STATUS_KIND);

	/**
	 * @param targetProcessStatusFlag    The flag indicating the desired (target)
	 *                                   process to run
	 * @param requiredProcessStatusFlags Required flags prior to running the desired
	 *                                   (target) process
	 * @return
	 */
	public List<String> getDocumentIdsInNeedOfProcessing(ProcessingStatusFlag targetProcessStatusFlag,
			Set<ProcessingStatusFlag> requiredProcessStatusFlags) {

		/*
		 * convert the required status flags into an array of PropertyFilters that match
		 * 'true'
		 */
		PropertyFilter[] requiredProcessStatusFlagFilters = requiredProcessStatusFlags.stream()
				.map(flag -> PropertyFilter.eq(flag.getDatastorePropertyName(), true)).collect(Collectors.toList())
				.toArray(new PropertyFilter[0]);

		/*
		 * require the target process to not have run, i.e. its flag == false AND the
		 * required process flags to all equal true
		 */
		CompositeFilter filter = CompositeFilter.and(
				PropertyFilter.eq(targetProcessStatusFlag.getDatastorePropertyName(), false),
				requiredProcessStatusFlagFilters);

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
	 * Executes a transaction with the Datastore to set the specified status flag to
	 * true
	 * 
	 * @param docId
	 * @param statusFlag
	 */
	public void setStatusTrue(String docId, ProcessingStatusFlag statusFlag) {
		String keyName = DatastoreProcessingStatusUtil.getStatusKeyName(docId);
		Key key = keyFactory.newKey(keyName);

		Transaction transaction = datastore.newTransaction();
		try {
			Entity status = transaction.get(key);
			if (status != null) {
				transaction.put(Entity.newBuilder(status).set(statusFlag.getDatastorePropertyName(), true).build());
			} else {
				throw new IllegalArgumentException(
						String.format("Unable to find status for key %s. Cannot update status for task:%s.", keyName,
								statusFlag.getDatastorePropertyName()));
			}
			transaction.commit();
		} finally {
			if (transaction.isActive()) {
				transaction.rollback();
			}
		}
	}

	public static String getStatusKeyName(String docId) {
		return String.format("%s.status", docId);
	}

}
