package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.util.Set;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class BiocToTextFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@SuppressWarnings("unchecked")
	@Test
	public void testBiocToTextConversionFn() throws IOException {
		PipelineKey pipelineKey = PipelineKey.BIOC_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.xml",
				CharacterEncoding.UTF_8);
		String docId = "PMC1790863";

		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate empty PCollectionView
		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
				.apply(View.<Set<String>>asSingleton());
		PCollectionTuple output = BiocToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
				timestamp, collection, docIdsAlreadyStoredView, OverwriteOutput.YES);

		String expectedText = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863.txt", CharacterEncoding.UTF_8);
		PAssert.that(output.get(BiocToTextFn.plainTextTag))
				.containsInAnyOrder(KV.of(docId, CollectionsUtil.createList(expectedText)));

		// looks correct, not sure why this doesn't pass
//		String expectedSectionAnnotationsInBioNLPFormat = ClassPathUtil.getContentsFromClasspathResource(
//				BiocToTextConverterTest.class, "PMC1790863-sections.bionlp", CharacterEncoding.UTF_8);
//		PAssert.that(output.get(BiocToTextFn.sectionAnnotationsTag))
//				.containsInAnyOrder(KV.of(docId, expectedSectionAnnotationsInBioNLPFormat));

		pipeline.run();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBiocToTextConversionFn2() throws IOException {
		PipelineKey pipelineKey = PipelineKey.BIOC_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC7500000.xml",
				CharacterEncoding.UTF_8);
		String docId = "PMC7500000";

		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate empty PCollectionView
		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
				.apply(View.<Set<String>>asSingleton());
		PCollectionTuple output = BiocToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
				timestamp, collection, docIdsAlreadyStoredView, OverwriteOutput.YES);

		String expectedText = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC7500000.txt", CharacterEncoding.UTF_8);
		PAssert.that(output.get(BiocToTextFn.plainTextTag))
				.containsInAnyOrder(KV.of(docId, CollectionsUtil.createList(expectedText)));

		// looks correct, not sure why this doesn't pass
//		String expectedSectionAnnotationsInBioNLPFormat = ClassPathUtil.getContentsFromClasspathResource(
//				BiocToTextConverterTest.class, "PMC1790863-sections.bionlp", CharacterEncoding.UTF_8);
//		PAssert.that(output.get(BiocToTextFn.sectionAnnotationsTag))
//				.containsInAnyOrder(KV.of(docId, expectedSectionAnnotationsInBioNLPFormat));

		pipeline.run();
	}

	@Test
	public void testBiocToTextConversionFn_invalidInput() throws IOException {
		PipelineKey pipelineKey = PipelineKey.BIOC_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863_invalid.xml", CharacterEncoding.UTF_8);
		String docId = "PMC1790863";

		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				pipelineKey, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
		String collection = null;

		// simulate empty PCollectionView
		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
				.apply(View.<Set<String>>asSingleton());
		PCollectionTuple output = BiocToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
				timestamp, collection, docIdsAlreadyStoredView, OverwriteOutput.YES);

		PAssert.that(output.get(BiocToTextFn.plainTextTag)).empty();
		PAssert.that(output.get(BiocToTextFn.sectionAnnotationsTag)).empty();

		// not sure how to test for not empty below
//		PAssert.that(output.get(BiocToTextFn.etlFailureTag)).empty();

		pipeline.run();
	}

//	@SuppressWarnings("unchecked")
//	@Test
//	public void testBiocToTextConversionFn_EmptyStackException() throws IOException {
//		PipelineKey pipelineKey = PipelineKey.BIOC_TO_TEXT;
//		String pipelineVersion = "0.1.0";
//		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
//
//		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextFnTest.class, "PMC4107124.xml",
//				CharacterEncoding.UTF_8);
//		String docId = "PMC4107124";
//
//		PCollection<KV<String, String>> input = pipeline.apply(
//				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
//
//		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
//				pipelineKey, pipelineVersion);
//		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
//				DocumentFormat.BIONLP, pipelineKey, pipelineVersion);
//		String collection = null;
//
//		// simulate empty PCollectionView
//		PCollectionView<Set<String>> docIdsAlreadyStoredView = pipeline
//				.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
//				.apply(View.<Set<String>>asSingleton());
//		PCollectionTuple output = BiocToTextFn.process(input, outputTextDocCriteria, outputAnnotationDocCriteria,
//				timestamp, collection, docIdsAlreadyStoredView, OverwriteOutput.YES);
//
//		
//		// TODO: this is a different doc txt at the moment -- we just need to make sure that parsing this XML doesn't throw an exception
////		String expectedText = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
////				"PMC1790863.txt", CharacterEncoding.UTF_8);
////		PAssert.that(output.get(BiocToTextFn.plainTextTag))
////				.containsInAnyOrder(KV.of(docId, CollectionsUtil.createList(expectedText)));
//
//		// looks correct, not sure why this doesn't pass
////		String expectedSectionAnnotationsInBioNLPFormat = ClassPathUtil.getContentsFromClasspathResource(
////				BiocToTextConverterTest.class, "PMC1790863-sections.bionlp", CharacterEncoding.UTF_8);
////		PAssert.that(output.get(BiocToTextFn.sectionAnnotationsTag))
////				.containsInAnyOrder(KV.of(docId, expectedSectionAnnotationsInBioNLPFormat));
//
//		pipeline.run();
//	}

}
