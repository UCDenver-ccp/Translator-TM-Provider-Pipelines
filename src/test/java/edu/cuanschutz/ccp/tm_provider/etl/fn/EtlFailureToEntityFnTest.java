package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.util.Arrays;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;

public class EtlFailureToEntityFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testEtlFailureToEntityConversionFn() throws IOException {
		/* test creation of a failure Cloud Datastore entity */
		String docId = "PMC1790863";
		String message = "Error!";
		String expectedMessage = "(This is a intential error made for testing) -- java.io.IOException: " + message;
		PipelineKey pipelineKey = PipelineKey.BIOC_TO_TEXT;
		String pipelineVersion = "0.1.0";
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		String stacktrace = null;
		Throwable t = null;
		try {
			throw new IOException(message);
		} catch (IOException e) {
			t = e;
			stacktrace = Arrays.toString(e.getStackTrace());
		}

		EtlFailureData failure = new EtlFailureData(pipelineKey, pipelineVersion,
				"(This is a intential error made for testing)", docId, DocumentType.BIOC, t, timestamp);

		PCollection<EtlFailureData> input = pipeline.apply(Create.of(failure));

		EtlFailureToEntityFn fn = new EtlFailureToEntityFn();
		PCollection<Entity> output = input.apply(ParDo.of(fn));
		Entity expectedEntity = EtlFailureToEntityFn.buildFailureEntity(pipelineKey, pipelineVersion, DocumentType.BIOC,
				docId, expectedMessage, stacktrace, timestamp);
		PAssert.that(output).containsInAnyOrder(expectedEntity);

		pipeline.run();
	}

}
