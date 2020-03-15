package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.google.protobuf.Empty;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * This function takes a combined-by-group-key result as input that is comprised
 * of all document identifiers that were scheduled to be processed and all of
 * the {@link ProcessingStatusFlag}s that need to be updated (set to true). It
 * updates all flags for a given document in a single transaction, which is
 * necessary in order to avoid the following Datastore exception:
 * com.google.cloud.datastore.DatastoreException: too much contention on these
 * datastore entities. please try again.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class UpdateStatusFn extends DoFn<KV<String, CoGbkResult>, Empty> {

	private static final long serialVersionUID = 1L;

	private final List<TupleTag<String>> tags;

	@ProcessElement
	public void processElement(@Element KV<String, CoGbkResult> docIdToUpdateFlags) {
		String docId = docIdToUpdateFlags.getKey();
		Set<ProcessingStatusFlag> flagsToUpdate = getFlagsToUpdate(docIdToUpdateFlags, tags);
		new DatastoreProcessingStatusUtil().setStatusTrue(docId, flagsToUpdate);
	}

	static Set<ProcessingStatusFlag> getFlagsToUpdate(KV<String, CoGbkResult> docIdToUpdateFlags,
			List<TupleTag<String>> tags) {
		CoGbkResult joinResult = docIdToUpdateFlags.getValue();

		Set<ProcessingStatusFlag> flagsToUpdate = new HashSet<ProcessingStatusFlag>();
		for (TupleTag<String> tag : tags) {
//			try {
			Iterable<String> flags = joinResult.getAll(tag);
			for (String flag : flags) {
				if (flag != ProcessingStatusFlag.NOOP.name()) {
					flagsToUpdate.add(ProcessingStatusFlag.valueOf(flag));
				}
			}
//			} catch (IllegalArgumentException e) {
//				//FIXME
//				// think it's possible for an IllegalArgumentException if a document contains no
//				// annotations for one of the ontologies. The exception is thrown by
//				// joinResults.getAll(tag). We simply let it pass and continue on. Not good form
//				// to use exception handling for logic, but there's no other obvious way.
//				// throw new RuntimeException("DocID: " + docId + "TAGS: " + tags.toString() +
//				// "\n\n\n", e);
//			}
		}
		return flagsToUpdate;
	}
}
