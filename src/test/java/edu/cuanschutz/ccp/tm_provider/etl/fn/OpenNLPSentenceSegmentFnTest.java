package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil.createProcessingStatus;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
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

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
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

		DocumentCriteria inputDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.FILE_LOAD, "v0.1.3");
		String documentText = ClassPathUtil.getContentsFromClasspathResource(BiocToTextFnTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		String docId = "PMC1790863";
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.SENTENCE_DONE;
//		Entity expectedEntity = createEntity(docId, targetProcessingStatusFlag);
		ProcessingStatus expectedEntity = createProcessingStatus(docId, targetProcessingStatusFlag);

		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();
		map.put(inputDocCriteria, documentText);

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> input = pipeline.apply(
				Create.of(KV.of(expectedEntity, map)).withCoder(KvCoder.of(SerializableCoder.of(ProcessingStatus.class),
						MapCoder.of(SerializableCoder.of(DocumentCriteria.class), StringUtf8Coder.of()))));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				pipelineKey, pipelineVersion);

		PCollectionTuple output = OpenNLPSentenceSegmentFn.process(input, outputTextDocCriteria, timestamp);

		String expectedBioNlp = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863-sentences.bionlp", CharacterEncoding.UTF_8);

		PCollection<EtlFailureData> failures = output.get(OpenNLPSentenceSegmentFn.ETL_FAILURE_TAG);
		PAssert.that(failures).satisfies(new FailureCountCheckerFn(0));

		PCollection<KV<ProcessingStatus, List<String>>> resultsCollection = output
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
			long count = StreamSupport.stream(input.spliterator(), false).count();
			assertEquals(String.format("There should only be %d items in the PCollection", expectedCount),
					expectedCount, count);
			return null;
		}

		protected abstract String getString(T item);

	}

	private static class MyPCollectionCountCheckerFn
			extends PCollectionCountCheckerFn<KV<ProcessingStatus, List<String>>> {

		private static final long serialVersionUID = 1L;

		public MyPCollectionCountCheckerFn(int expectedCount) {
			super(expectedCount);
		}

		@Override
		protected String getString(KV<ProcessingStatus, List<String>> item) {
			ProcessingStatus statusEntity = item.getKey();
			return String.format("%s -- %s", statusEntity.getDocumentId(), item.getValue().toString());
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
