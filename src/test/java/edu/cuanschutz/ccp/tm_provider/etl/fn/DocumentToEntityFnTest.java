package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
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
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.SerializableFunction;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class DocumentToEntityFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testBiocDocumentToEntityConversionFn() throws IOException {
		/* test creation of a bioc document Cloud Datastore entity */
		String collection = "PMC";
		Set<String> collections = CollectionsUtil.createSet(collection);
		collections.add(ToEntityFnUtils.getDateCollectionName());
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.xml",
				CharacterEncoding.UTF_8);
		PCollection<KV<String, List<String>>> input = pipeline
				.apply(Create.of(KV.of(docId, CollectionsUtil.createList(biocXml)))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))));

		DocumentCriteria dc = new DocumentCriteria(DocumentType.BIOC, DocumentFormat.BIOCXML, PipelineKey.ORIG,
				pipelineVersion);
		DocumentToEntityFn fn = new DocumentToEntityFn(dc, collection);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, 0, 1, dc, biocXml, collections);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

	@Test
	public void testPlainTextDocumentToEntityConversionFn() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String collection = "PMC";
		Set<String> collections = CollectionsUtil.createSet(collection);
		collections.add(ToEntityFnUtils.getDateCollectionName());
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String text = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		PCollection<KV<String, List<String>>> input = pipeline
				.apply(Create.of(KV.of(docId, CollectionsUtil.createList(text)))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))));

		DocumentCriteria dc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				pipelineVersion);

		DocumentToEntityFn fn = new DocumentToEntityFn(dc, collection);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, 0, 1, dc, text, collections);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

	@Test
	public void testAnnotationDocumentToEntityConversionFn() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String collection = "PMC";
		Set<String> collections = CollectionsUtil.createSet(collection);
		collections.add(ToEntityFnUtils.getDateCollectionName());
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String sectionAnnotationsInBioNLPFormat = ClassPathUtil.getContentsFromClasspathResource(
				BiocToTextConverterTest.class, "PMC1790863-sections.bionlp", CharacterEncoding.UTF_8);
		PCollection<KV<String, List<String>>> input = pipeline
				.apply(Create.of(KV.of(docId, CollectionsUtil.createList(sectionAnnotationsInBioNLPFormat)))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))));

		DocumentCriteria dc = new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.BIOC_TO_TEXT, pipelineVersion);
		DocumentToEntityFn fn = new DocumentToEntityFn(dc, collection);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, 0, 1, dc, sectionAnnotationsInBioNLPFormat, collections);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

	@Test
	public void testPlainTextDocumentToEntityConversionFn2() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String collection = "PMC";
		Set<String> collections = CollectionsUtil.createSet(collection);
		collections.add(ToEntityFnUtils.getDateCollectionName());
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String text = "this is some text";
		PCollection<KV<String, List<String>>> input = pipeline
				.apply(Create.of(KV.of(docId, CollectionsUtil.createList(text)))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))));

		DocumentCriteria dc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				pipelineVersion);

		DocumentToEntityFn fn = new DocumentToEntityFn(dc, collection);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, 0, 1, dc, text, collections);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}
	
	
	
	@Test
	public void testPlainTextDocumentToEntityConversionFnWithAdditionalCollectionName() throws IOException {
		/* test creation of a plain text document Cloud Datastore entity */
		String collection = "PMC";
		Set<String> collections = CollectionsUtil.createSet(collection, "NEW COLLECTION: PMC1790863");
		collections.add(ToEntityFnUtils.getDateCollectionName());
		String docId = "PMC1790863";
		String pipelineVersion = "0.1.0";
		String text = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		PCollection<KV<String, List<String>>> input = pipeline
				.apply(Create.of(KV.of(docId, CollectionsUtil.createList(text)))
						.withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))));

		DocumentCriteria dc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				pipelineVersion);

		
		SerializableFunction<String, String> collectionFn = parameter -> "NEW COLLECTION: " + parameter;
		
		DocumentToEntityFn fn = new DocumentToEntityFn(dc, collection, collectionFn);
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = DocumentToEntityFn.createEntity(docId, 0, 1, dc, text, collections);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

}
