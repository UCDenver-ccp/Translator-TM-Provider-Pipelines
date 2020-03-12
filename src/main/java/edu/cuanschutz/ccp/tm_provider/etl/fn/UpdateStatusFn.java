package edu.cuanschutz.ccp.tm_provider.etl.fn;

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
 * of all document identifiers that were scheduled to be processed and all
 * document identifiers that resulted in failures. For the document identifiers
 * that did not result in failure, it updates the status of the specified
 * {@link ProcessStatusFlag} in Cloud Datastore (setting the flag to true). 
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class UpdateStatusFn extends DoFn<KV<String, CoGbkResult>, Empty> {

	private static final long serialVersionUID = 1L;

	private final ProcessingStatusFlag statusFlag;
	private final TupleTag<Boolean> allTag;
	private final TupleTag<Boolean> failedTag;

	@ProcessElement
	public void processElement(@Element KV<String, CoGbkResult> docIdToUpdateFlags, OutputReceiver<Empty> out) {
		String docId = docIdToUpdateFlags.getKey();
		CoGbkResult joinResult = docIdToUpdateFlags.getValue();

		Iterable<Boolean> all = joinResult.getAll(allTag);
		Iterable<Boolean> failed = joinResult.getAll(failedTag);

		sanityCheck(all, failed);

		// there should only be one Boolean in each iterable. 'all' will always be true,
		// so we only need to check 'failed'
		boolean processFailed = false;
		if (failed.iterator().hasNext()) {
			processFailed = failed.iterator().next();
		}

		// if the process did not log a failure, then updated the status for the
		// specified statusFlag
		if (!processFailed) {
			new DatastoreProcessingStatusUtil().setStatusTrue(docId, statusFlag);
		}

	}

	/**
	 * Sanity check function - we expect only one item in each iterable, and this
	 * makes sure that assumption holds true.
	 * 
	 * @param all
	 * @param failed
	 */
	private void sanityCheck(Iterable<Boolean> all, Iterable<Boolean> failed) {
		// check to make sure there is only one item per iterable
		int count = 0;
		for (@SuppressWarnings("unused")
		Boolean b : all) {
			count++;
		}
		if (count > 1) {
			throw new RuntimeException(String.format("size of all: %d", count));
		}
		count = 0;
		for (@SuppressWarnings("unused")
		Boolean b : failed) {
			count++;
		}
		if (count > 1) {
			throw new RuntimeException(String.format("size of failed: %d", count));
		}
	}

}
