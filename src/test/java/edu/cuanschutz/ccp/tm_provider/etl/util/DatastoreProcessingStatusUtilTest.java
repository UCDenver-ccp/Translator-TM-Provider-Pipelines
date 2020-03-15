package edu.cuanschutz.ccp.tm_provider.etl.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;

public class DatastoreProcessingStatusUtilTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testCombineStatusLists() {

		String docId1 = "PMC1";
		String docId2 = "PMC2";

		PCollection<KV<String, String>> ogerOutput_chebi = pipeline.apply("chebi-out",
				Create.of(KV.of(docId1, ProcessingStatusFlag.OGER_CHEBI_DONE.name()),
						KV.of(docId2, ProcessingStatusFlag.NOOP.name()))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
		PCollection<KV<String, String>> ogerOutput_cl = pipeline.apply("cl-out",
				Create.of(KV.of(docId1, ProcessingStatusFlag.OGER_CL_DONE.name()),
						KV.of(docId2, ProcessingStatusFlag.OGER_CL_DONE.name()))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
		PCollection<KV<String, String>> ogerOutput_pr = pipeline.apply("pr-out",
				Create.of(KV.of(docId1, ProcessingStatusFlag.OGER_PR_DONE.name()),
						KV.of(docId2, ProcessingStatusFlag.OGER_PR_DONE.name()))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		List<PCollection<KV<String, String>>> statusList = Arrays.asList(ogerOutput_chebi, ogerOutput_cl,
				ogerOutput_pr);

		List<TupleTag<String>> tags = new ArrayList<TupleTag<String>>();
		PCollection<KV<String, CoGbkResult>> combinedStatus = DatastoreProcessingStatusUtil
				.combineStatusLists(statusList, tags);

		PAssert.thatMap(combinedStatus).satisfies(results -> {

			assertEquals("should be 2 b/c there are 2 documents.", 2, results.size());
			assertEquals("keys should be doc Ids", new HashSet<String>(Arrays.asList(docId1, docId2)),
					results.keySet());

			CoGbkResult result_docId1 = results.get(docId1);
			assertEquals(ProcessingStatusFlag.OGER_CHEBI_DONE.name(), result_docId1.getOnly(tags.get(0)));
			assertEquals(ProcessingStatusFlag.OGER_CL_DONE.name(), result_docId1.getOnly(tags.get(1)));
			assertEquals(ProcessingStatusFlag.OGER_PR_DONE.name(), result_docId1.getOnly(tags.get(2)));

			CoGbkResult result_docId2 = results.get(docId2);
			assertEquals(ProcessingStatusFlag.NOOP.name(), result_docId2.getOnly(tags.get(0)));
			assertEquals(ProcessingStatusFlag.OGER_CL_DONE.name(), result_docId2.getOnly(tags.get(1)));
			assertEquals(ProcessingStatusFlag.OGER_PR_DONE.name(), result_docId2.getOnly(tags.get(2)));

			return null;
		});

		pipeline.run();

	}

	@Test
	public void testGetSuccessStatus() {
		ProcessingStatusFlag processingStatusFlag = ProcessingStatusFlag.OGER_CHEBI_DONE;

		String docId1 = "doc1";
		String docId2 = "doc2";
		String docId3 = "doc3";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Throwable t = null;
		try {
			throw new IOException("Error!");
		} catch (IOException e) {
			t = e;
		}

		EtlFailureData failure = new EtlFailureData(PipelineKey.OGER, "0.1.0", "custom message", docId2,
				DocumentType.CONCEPT_CHEBI, t, timestamp);

		PCollection<EtlFailureData> failures = pipeline.apply("create failures", Create.of(failure));
		PCollection<String> processedDocIds = pipeline.apply("create doc ids", Create.of(docId1, docId2, docId3));
		;

		PCollection<KV<String, String>> successStatus = DatastoreProcessingStatusUtil.getSuccessStatus(processedDocIds,
				failures, processingStatusFlag);

		PAssert.thatMap(successStatus).satisfies(results -> {

			assertEquals("should be 3 b/c there are 3 documents ", 3, results.size());
			assertEquals("keys should be doc Ids", new HashSet<String>(Arrays.asList(docId1, docId2, docId3)),
					results.keySet());

			String result_docId1 = results.get(docId1);
			assertEquals(ProcessingStatusFlag.OGER_CHEBI_DONE.name(), result_docId1);

			String result_docId2 = results.get(docId2);
			assertEquals(ProcessingStatusFlag.NOOP.name(), result_docId2);

			String result_docId3 = results.get(docId3);
			assertEquals(ProcessingStatusFlag.OGER_CHEBI_DONE.name(), result_docId3);

			return null;
		});

		pipeline.run();

	}

}
