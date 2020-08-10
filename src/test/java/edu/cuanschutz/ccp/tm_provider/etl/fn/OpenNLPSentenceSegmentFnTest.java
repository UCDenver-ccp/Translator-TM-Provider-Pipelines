package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class OpenNLPSentenceSegmentFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@SuppressWarnings("unchecked")
	@Test
	public void testSentenceSegmentation() throws IOException {
		PipelineKey pipelineKey = PipelineKey.SENTENCE_SEGMENTATION;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		String docId = "PMC1790863";

		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.BIONLP,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;
		PCollectionTuple output = OpenNLPSentenceSegmentFn.process(input, outputTextDocCriteria, timestamp);

		String expectedBioNlp = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863-sentences.bionlp",
				CharacterEncoding.UTF_8);
		PAssert.that(output.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG))
				.containsInAnyOrder(KV.of(docId, CollectionsUtil.createList(expectedBioNlp)));

		pipeline.run();
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
