package edu.cuanschutz.ccp.tm_provider.etl.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * This function takes a combined-by-group-key result as input that is comprised
 * of all document identifiers that were scheduled to be processed and all
 * document identifiers that resulted in failures. It returns KV pairs mapping
 * document ID to the {@link ProcessingStatusFlag} for the document IDs that
 * were successfully processed.
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class SuccessStatusFn extends DoFn<KV<String, CoGbkResult>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	private final ProcessingStatusFlag statusFlag;
	private final TupleTag<Boolean> allTag;
	private final TupleTag<Boolean> failedTag;

	@ProcessElement
	public void processElement(@Element KV<String, CoGbkResult> docIdToUpdateFlags,
			OutputReceiver<KV<String, String>> out) {
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

		// if the process did not log a failure, then output it paired the status flag
		// paired with the document id. It will get updated downstream so that the
		// status updates can be batched (1 per document)

		if (processFailed) {
			out.output(KV.of(docId, ProcessingStatusFlag.NOOP.name()));
		} else {
			out.output(KV.of(docId, statusFlag.name()));
//			new DatastoreProcessingStatusUtil().setStatusTrue(docId, statusFlagsToUpdate);
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
