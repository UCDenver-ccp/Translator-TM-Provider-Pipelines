package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
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

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class ExtractContentFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@SuppressWarnings("unchecked")
	@Test
	public void test() throws UnsupportedEncodingException {
		PipelineKey pipelineKey = PipelineKey.FILE_LOAD;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String[] plainText = getPlainText();

		String docId = "PMC1790863";
		String docText = plainText[0] + plainText[1];

		PCollection<KV<String, String>> input = pipeline.apply(
				Create.of(KV.of(docId, docText)).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, pipelineKey,
				pipelineVersion);
		String collection = "CORD19";

		PCollectionTuple output = ExtractContentFn.process(input, outputDocCriteria, timestamp, ".txt", collection,
				ProcessingStatusFlag.TEXT_DONE);

		List<String> chunks = CollectionsUtil.createList(plainText[0], plainText[1]);

		// check that there are 2 chunks
		PAssert.that(output.get(ExtractContentFn.contentTag)).containsInAnyOrder(KV.of(docId, chunks));

		// check that the status logged two chunks and the collection name
		ProcessingStatus statusEntity = new ProcessingStatus(docId);
		statusEntity.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		statusEntity.addCollection(collection);
		PAssert.that(output.get(ExtractContentFn.processingStatusTag)).containsInAnyOrder(statusEntity);

		pipeline.run();
	}

	private String[] getPlainText() throws UnsupportedEncodingException {
		StringBuffer s = new StringBuffer();

		for (int i = 0; i < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES - 1; i++) {
			s.append("a");
		}

		String firstStr = s.toString();

		assertEquals("one less than the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// Add a UTF-8 char that is multi-byte and it should jump to two strings
		String secondStr = "\u0190";
		s.append(secondStr);
		assertEquals(
				"equal to the threshold in character count, but greater than the byte count threshold, so should be two strings",
				2, PipelineMain.chunkContent(s.toString()).size());

		return new String[] { firstStr, secondStr };

	}

}
