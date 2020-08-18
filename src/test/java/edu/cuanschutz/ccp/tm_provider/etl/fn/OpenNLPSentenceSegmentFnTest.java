package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil.createEntity;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;

public class OpenNLPSentenceSegmentFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@SuppressWarnings("unchecked")
	@Test
	public void testSentenceSegmentation() throws IOException {
		PipelineKey pipelineKey = PipelineKey.SENTENCE_SEGMENTATION;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String documentText = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863.txt", CharacterEncoding.UTF_8);
		String docId = "PMC1790863";
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.SENTENCE_DONE;
		Entity expectedEntity = createEntity(docId, targetProcessingStatusFlag);

		PCollection<KV<Entity, String>> input = pipeline.apply(Create.of(KV.of(expectedEntity, documentText))
				.withCoder(KvCoder.of(SerializableCoder.of(Entity.class), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.BIONLP,
				pipelineKey, pipelineVersion);

		PCollectionTuple output = OpenNLPSentenceSegmentFn.process(input, outputTextDocCriteria, timestamp);

		String expectedBioNlp = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863-sentences.bionlp", CharacterEncoding.UTF_8);

		PCollection<EtlFailureData> failures = output.get(OpenNLPSentenceSegmentFn.ETL_FAILURE_TAG);
		PAssert.that(failures).satisfies(new FailureCountCheckerFn(0));

		PCollection<KV<Entity, List<String>>> resultsCollection = output
				.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG);
		PAssert.that(resultsCollection).satisfies(new MyPCollectionCountCheckerFn(1));

		PAssert.that(output.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG))
				.containsInAnyOrder(KV.of(expectedEntity, CollectionsUtil.createList(expectedBioNlp)));

		pipeline.run();
	}

	@Data
	private abstract static class PCollectionCountCheckerFn<T> implements SerializableFunction<Iterable<T>, Void> {

		private static final long serialVersionUID = 1L;
		private final int expectedCount;

		@Override
		public Void apply(Iterable<T> input) {
			int count = 0;
			for (T item : input) {
				count++;
			}
			assertEquals(String.format("There should only be %d items in the PCollection", expectedCount),
					expectedCount, count);
			return null;
		}

		protected abstract String getString(T item);

	}

	private static class MyPCollectionCountCheckerFn extends PCollectionCountCheckerFn<KV<Entity, List<String>>> {

		private static final long serialVersionUID = 1L;

		public MyPCollectionCountCheckerFn(int expectedCount) {
			super(expectedCount);
		}

		@Override
		protected String getString(KV<Entity, List<String>> item) {
			Entity statusEntity = item.getKey();
			return String.format("%s -- %s", DatastoreProcessingStatusUtil.getDocumentId(statusEntity),
					item.getValue().toString());
		}

	}

	private static class FailureCountCheckerFn extends PCollectionCountCheckerFn<EtlFailureData> {

		private static final long serialVersionUID = 1L;

		public FailureCountCheckerFn(int expectedCount) {
			super(expectedCount);
		}

		@Override
		protected String getString(EtlFailureData item) {
			return String.format("%s -- %s", item.getDocumentId(), item.getMessage());
		}

	}

	@Test
	public void testSegmentSentences() throws IOException {
		String s = "This is a sentence. Here is another sentence.";
		TextDocument td = OpenNLPSentenceSegmentFn.segmentSentences(s);

		List<TextAnnotation> sentences = td.getAnnotations();

		assertEquals(2, sentences.size());

	}

	@Test
	public void testSegmentSentencesWithLineBreak() throws IOException {
		String s = "This is a sentence\n Here is another sentence.";
		TextDocument td = OpenNLPSentenceSegmentFn.segmentSentences(s);

		List<TextAnnotation> sentences = td.getAnnotations();

		assertEquals(2, sentences.size());

	}

	@Test
	public void testSegmentSentencesWithMultipleLineBreaks() throws IOException {
		String s = "This is a sentence\n\n\n\n Here is another sentence.";
		TextDocument td = OpenNLPSentenceSegmentFn.segmentSentences(s);

		List<TextAnnotation> sentences = td.getAnnotations();

		assertEquals(2, sentences.size());

	}

}
