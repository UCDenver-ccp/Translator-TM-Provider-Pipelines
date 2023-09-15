package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

public class UpdateStatusFnTest {

	@Test
	public void testGetFlagsToUpdate() {

		TupleTag<String> chebiTag = new TupleTag<String>();
		TupleTag<String> clTag = new TupleTag<String>();
		TupleTag<String> prTag = new TupleTag<String>();

		List<TupleTag<String>> tags = Arrays.asList(chebiTag, clTag, prTag);

		List<String> chebiList = Arrays.asList(ProcessingStatusFlag.OGER_CHEBI_DONE.name());
		List<String> clList = Arrays.asList(ProcessingStatusFlag.NOOP.name());
		List<String> prList = Arrays.asList(ProcessingStatusFlag.OGER_PR_DONE.name());
		CoGbkResult combined = CoGbkResult.of(chebiTag, chebiList).and(clTag, clList).and(prTag, prList);

		String docId = "PMC12345";
		KV<String, CoGbkResult> docIdToUpdateFlags = KV.of(docId, combined);

		Set<ProcessingStatusFlag> flagsToUpdate = UpdateStatusFn.getFlagsToUpdate(docIdToUpdateFlags, tags);

		Set<ProcessingStatusFlag> expectedFlags = new HashSet<ProcessingStatusFlag>(
				Arrays.asList(ProcessingStatusFlag.OGER_CHEBI_DONE, ProcessingStatusFlag.OGER_PR_DONE));

		assertEquals(expectedFlags, flagsToUpdate);
	}

}
