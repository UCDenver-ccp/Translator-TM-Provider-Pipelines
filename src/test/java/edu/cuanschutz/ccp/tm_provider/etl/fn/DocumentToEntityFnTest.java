package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class DocumentToEntityFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testBiocDocumentToEntityConversionFn() throws IOException {
		/* test creation of a bioc document Cloud Datastore entity */
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.xml",
				CharacterEncoding.UTF_8);
		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, biocXml)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentToEntityFn fn = new DocumentToEntityFn(DocumentType.BIOC, DocumentFormat.BIOCXML, PipelineKey.ORIG,
				pipelineVersion);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, DocumentType.BIOC, DocumentFormat.BIOCXML,
				PipelineKey.ORIG, pipelineVersion, biocXml);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

	@Test
	public void testPlainTextDocumentToEntityConversionFn() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String text = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		PCollection<KV<String, String>> input = pipeline
				.apply(Create.of(KV.of(docId, text)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentToEntityFn fn = new DocumentToEntityFn(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				pipelineVersion);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.BIOC_TO_TEXT, pipelineVersion, text);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

	@Test
	public void testAnnotationDocumentToEntityConversionFn() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String sectionAnnotationsInBioNLPFormat = ClassPathUtil.getContentsFromClasspathResource(
				BiocToTextConverterTest.class, "PMC1790863-sections.bionlp", CharacterEncoding.UTF_8);
		PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of(docId, sectionAnnotationsInBioNLPFormat))
				.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentToEntityFn fn = new DocumentToEntityFn(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.BIOC_TO_TEXT, pipelineVersion);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.BIOC_TO_TEXT, pipelineVersion, sectionAnnotationsInBioNLPFormat);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

}
