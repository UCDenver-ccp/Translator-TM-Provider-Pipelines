package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

public class ProcessingStatusToEntityFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessingStatusToEntityConversionFn() throws IOException {
		/* test creation of a processing status Cloud Datastore entity */
		String docId = "PMC1790863";

		ProcessingStatus status = new ProcessingStatus(docId);

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.OGER_CL_DONE);

		PCollection<ProcessingStatus> input = pipeline.apply(Create.of(status));
		PCollection<KV<String,Entity>> output = input.apply(ParDo.of(new ProcessingStatusToEntityFn()));
		Entity expectedEntity = ProcessingStatusToEntityFn.buildStatusEntity(status);
		PAssert.that(output).containsInAnyOrder(KV.of(expectedEntity.getKey().toString(), expectedEntity));

		pipeline.run();
	}

	
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessingStatusToEntityConversionFnWithPubYearAndTypes() throws IOException {
		/* test creation of a processing status Cloud Datastore entity */
		String docId = "PMC1790863";

		ProcessingStatus status = new ProcessingStatus(docId);
		status.setYearPublished("1997");
		status.addPublicationType("journal article");
		status.addPublicationType("review");

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.OGER_CL_DONE);

		PCollection<ProcessingStatus> input = pipeline.apply(Create.of(status));
		PCollection<KV<String,Entity>> output = input.apply(ParDo.of(new ProcessingStatusToEntityFn()));
		Entity expectedEntity = ProcessingStatusToEntityFn.buildStatusEntity(status);
		PAssert.that(output).containsInAnyOrder(KV.of(expectedEntity.getKey().toString(), expectedEntity));

		pipeline.run();
	}
}
