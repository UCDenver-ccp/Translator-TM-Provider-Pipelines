package edu.cuanschutz.ccp.tm_provider.etl;

import static edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil.createEntity;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import lombok.Data;

public class PipelineMainTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testChunkString() throws UnsupportedEncodingException {
		StringBuffer s = new StringBuffer();

		for (int i = 0; i < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES; i++) {
			s.append("a");
		}

		assertEquals("equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// add one more character and it should now return 2 strings
		s.append("b");
		assertEquals("equal to the threshold, so should be two strings", 2,
				PipelineMain.chunkContent(s.toString()).size());

		// remove two characters (in this case the first and second) and it should
		// return to 1 string
		s = s.deleteCharAt(0);
		s = s.deleteCharAt(0);
		assertEquals("back to equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// we are now at one short of the threshold. Add a UTF-8 char that is multi-byte
		// and it should jump to two strings
		s.append("\u0190");
		assertEquals(
				"equal to the threshold in character count, but greater than the byte count threshold, so should be two strings",
				2, PipelineMain.chunkContent(s.toString()).size());

	}

	@Data
	private static class PCollectionCountCheckerFn<T> implements SerializableFunction<Iterable<T>, Void> {

		private static final long serialVersionUID = 1L;
		private final int expectedCount;

		@Override
		public Void apply(Iterable<T> input) {
			long count = StreamSupport.stream(input.spliterator(), false).count();
			assertEquals(String.format("There should only be %d items in the PCollection", expectedCount),
					expectedCount, count);
			return null;
		}

	}

	@Test
	public void testDeduplicateDocumentsStringKey() {

		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

		List<KV<String, List<String>>> input = Arrays.asList(KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId2, Arrays.asList("doc2 text")), KV.of(docId3, Arrays.asList("doc3 text")),
				KV.of(docId4, Arrays.asList("doc4 text")), KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId1, Arrays.asList("doc1 text")), KV.of(docId2, Arrays.asList("doc2 text")),
				KV.of(docId2, Arrays.asList("doc2 text")));

		assertEquals("there should only be 4 unique entries in the input", 4,
				new HashSet<KV<String, List<String>>>(input).size());

		PCollection<KV<String, List<String>>> statusEntityToDocContent = pipeline.apply(Create.of(input));
		PCollection<KV<String, List<String>>> nonredundantStatusEntityToDocContent = PipelineMain
				.deduplicateDocumentsByStringKey(statusEntityToDocContent);

		PAssert.that(nonredundantStatusEntityToDocContent)
				.satisfies(new PCollectionCountCheckerFn<KV<String, List<String>>>(4));

		pipeline.run();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDeduplicateDocuments() {

		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

		Entity entity1 = createEntity(docId1, ProcessingStatusFlag.TEXT_DONE);
		Entity entity2 = createEntity(docId2, ProcessingStatusFlag.TEXT_DONE);
		Entity entity3 = createEntity(docId3, ProcessingStatusFlag.TEXT_DONE);
		Entity entity4 = createEntity(docId4, ProcessingStatusFlag.TEXT_DONE);

		List<KV<Entity, List<String>>> input = Arrays.asList(KV.of(entity1, Arrays.asList("doc1 text")),
				KV.of(entity2, Arrays.asList("doc2 text")), KV.of(entity3, Arrays.asList("doc3 text")),
				KV.of(entity4, Arrays.asList("doc4 text")), KV.of(entity1, Arrays.asList("doc1 text")),
				KV.of(entity1, Arrays.asList("doc1 text")), KV.of(entity2, Arrays.asList("doc2 text")),
				KV.of(entity2, Arrays.asList("doc2 text")));

		assertEquals("there should only be 4 unique entries in the input", 4,
				new HashSet<KV<Entity, List<String>>>(input).size());

		PCollection<KV<Entity, List<String>>> statusEntityToDocContent = pipeline.apply(Create.of(input));
		PCollection<KV<String, List<String>>> nonredundantStatusEntityToDocContent = PipelineMain
				.deduplicateDocuments(statusEntityToDocContent);

		PAssert.that(nonredundantStatusEntityToDocContent)
				.satisfies(new PCollectionCountCheckerFn<KV<String, List<String>>>(4));

		PAssert.that(nonredundantStatusEntityToDocContent).containsInAnyOrder(KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId2, Arrays.asList("doc2 text")), KV.of(docId3, Arrays.asList("doc3 text")),
				KV.of(docId4, Arrays.asList("doc4 text")));

		pipeline.run();
	}

	@Test
	public void testDeduplicateStatusEnititie() {
		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

		Entity entity1 = createEntity(docId1, ProcessingStatusFlag.TEXT_DONE);
		Entity entity2 = createEntity(docId2, ProcessingStatusFlag.TEXT_DONE);
		Entity entity3 = createEntity(docId3, ProcessingStatusFlag.TEXT_DONE);
		Entity entity4 = createEntity(docId4, ProcessingStatusFlag.TEXT_DONE);

		List<Entity> input = Arrays.asList(entity1, entity2, entity3, entity4, entity1, entity3, entity4);

		assertEquals("there should only be 4 unique entries in the input", 4, new HashSet<Entity>(input).size());

		PCollection<Entity> statusEntities = pipeline.apply(Create.of(input));

		PCollection<Entity> nonredundantEntities = PipelineMain.deduplicateStatusEntities(statusEntities);

		PAssert.that(nonredundantEntities).satisfies(new PCollectionCountCheckerFn<Entity>(4));

		PAssert.that(nonredundantEntities).containsInAnyOrder(entity1, entity2, entity3, entity4);

		pipeline.run();

	}

}
