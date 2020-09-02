package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.SentenceCooccurrenceBuilderTest;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class SentenceCooccurrenceFileBuilderFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	private Map<DocumentType, String> docTypeToContent;
	private String conceptChebi;
	private String conceptCl;
	private String dependency;
//	private String sentences;
	private String text;

	@Before
	public void setUp() throws IOException {
		docTypeToContent = new HashMap<DocumentType, String>();
		conceptChebi = ClassPathUtil.getContentsFromClasspathResource(SentenceCooccurrenceBuilderTest.class,
				"12345.concept_chebi.bionlp", CharacterEncoding.UTF_8);
		conceptCl = ClassPathUtil.getContentsFromClasspathResource(SentenceCooccurrenceBuilderTest.class,
				"12345.concept_cl.bionlp", CharacterEncoding.UTF_8);
		dependency = ClassPathUtil.getContentsFromClasspathResource(SentenceCooccurrenceBuilderTest.class,
				"12345.dependency.conllu", CharacterEncoding.UTF_8);
//		sentences = ClassPathUtil.getContentsFromClasspathResource(SentenceCooccurrenceBuilderTest.class,
//				"12345.sentence.bionlp", CharacterEncoding.UTF_8);
		text = ClassPathUtil.getContentsFromClasspathResource(SentenceCooccurrenceBuilderTest.class, "12345.txt",
				CharacterEncoding.UTF_8);

		docTypeToContent.put(DocumentType.TEXT, text);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.DEPENDENCY_PARSE, dependency);
	}

	@Ignore("unable to serialize -- IllegalArgumentException -- not sure why")
	@SuppressWarnings("unchecked")
	@Test
	public void testSentenceCooccurrenceFn() throws IOException {

		PipelineKey pipelineKey = PipelineKey.SENTENCE_SEGMENTATION;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

//		String biocXml = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class, "PMC1790863.txt",
//				CharacterEncoding.UTF_8);
		String docId = "PMC1790863";

//		AtomicCoder<DocumentType> docTypeCoder = new AtomicCoder<DocumentType>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void encode(DocumentType value, OutputStream outStream) throws CoderException, IOException {
//				StringUtf8Coder.of().encode(value.name(), outStream);
//
//			}
//
//			@Override
//			public DocumentType decode(InputStream inStream) throws CoderException, IOException {
//				String decode = StringUtf8Coder.of().decode(inStream);
//				return DocumentType.valueOf(decode.toUpperCase());
//			}
//		};

		AtomicCoder<Map<DocumentType, String>> mapCoder = new AtomicCoder<Map<DocumentType, String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void encode(Map<DocumentType, String> value, OutputStream outStream)
					throws CoderException, IOException {
				StringBuilder builder = new StringBuilder();

				for (Entry<DocumentType, String> entry : value.entrySet()) {
					builder.append(entry.getKey().name() + "|" + entry.getValue() + "\t");
				}

				StringUtf8Coder.of().encode(builder.toString(), outStream);

			}

			@Override
			public Map<DocumentType, String> decode(InputStream inStream) throws CoderException, IOException {
				String decode = StringUtf8Coder.of().decode(inStream);
				// drop final tab
				int l = decode.length();
				decode = decode.substring(0, decode.length() - 1);

				if (decode.length() != (l - 1)) {
					throw new RuntimeException("substring wrong");
				}

				Map<DocumentType, String> map = new HashMap<DocumentType, String>();
				for (String entry : decode.split("\\t")) {
					String[] split = entry.split("\\|");
					DocumentType dt = DocumentType.valueOf(split[0]);
					String s = split[1];
					map.put(dt, s);
				}

				return map;
			}

		};

//		PCollection<KV<DocumentType, String>> apply = pipeline
//				.apply(Create.of(map).withCoder(KvCoder.of(docTypeCoder, StringUtf8Coder.of())));

		PCollection<KV<String, Map<DocumentType, String>>> input = pipeline
				.apply(Create.of(KV.of(docId, docTypeToContent)).withCoder(KvCoder.of(StringUtf8Coder.of(), mapCoder)));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.BIONLP,
				pipelineKey, pipelineVersion);

		PCollectionTuple output = SentenceCooccurrenceFileBuilderFn.process(input, outputTextDocCriteria, timestamp);

		String expectedBioNlp = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863-sentences.bionlp", CharacterEncoding.UTF_8);
		PAssert.that(output.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG))
				.containsInAnyOrder(KV.of(
						PipelineTestUtil.createProcessingStatus(docId, ProcessingStatusFlag.SENTENCE_COOCCURRENCE_EXPORT_DONE),
						CollectionsUtil.createList(expectedBioNlp)));

		pipeline.run();
	}

}
