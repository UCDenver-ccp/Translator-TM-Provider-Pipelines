package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.apache.beam.sdk.io.xml.JAXBCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.medline.PubmedArticle;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.update.UpdateMedlineEntitiesPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

public class MedlineStatusUpdateFnTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testUpdateProcessingStatusWithYearAndPublicationTypes()
			throws JAXBException, IOException, XMLStreamException {
		String pmid1 = "PMID:1";
		String pmid2 = "PMID:31839728";
		String pmid3 = "PMID:31839729";

		PCollection<PubmedArticle> pubmedArticles = pipeline.apply("create pubmed articles", Create
				.of(MedlineXmlToTextFnTest.getSamplePubmedArticles(MedlineXmlToTextFnTest.SAMPLE_PUBMED20N0001_XML_GZ))
				.withCoder(JAXBCoder.of(PubmedArticle.class)));

		PCollection<KV<String, PubmedArticle>> pubmedArticlesInput = UpdateMedlineEntitiesPipeline
				.keyByPubmedId(pubmedArticles);

		ProcessingStatus status1 = new ProcessingStatus(pmid1);
		status1.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		ProcessingStatus status2 = new ProcessingStatus(pmid2);
		status2.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		ProcessingStatus status3 = new ProcessingStatus(pmid3);
		status3.enableFlag(ProcessingStatusFlag.TEXT_DONE);

		Map<String, ProcessingStatus> map = new HashMap<String, ProcessingStatus>();
		map.put(pmid1, status1);
		map.put(pmid2, status2);
		map.put(pmid3, status3);

		PCollection<KV<String, ProcessingStatus>> processingStatusInput = pipeline
				.apply("create processing status input", Create.of(map));

		/* group the lines by their ids */
		final TupleTag<PubmedArticle> articleTag = new TupleTag<>();
		final TupleTag<ProcessingStatus> statusEntityTag = new TupleTag<>();
		PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(articleTag, pubmedArticlesInput)
				.and(statusEntityTag, processingStatusInput).apply("merge by pmid", CoGroupByKey.create());

		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		PCollectionTuple output = MedlineStatusUpdateFn.process(result, articleTag, statusEntityTag, timestamp);

		PCollection<ProcessingStatus> outputStatus = output.get(MedlineStatusUpdateFn.processingStatusTag);

		String expectedYear1 = "1975";
		List<String> expectedPublicationTypes1 = Arrays.asList("Journal Article",
				"Research Support, U.S. Gov't, P.H.S.");

		String expectedYear2 = "2020";
		List<String> expectedPublicationTypes2 = Arrays.asList("Case Reports");

		String expectedYear3 = "2020";
		List<String> expectedPublicationTypes3 = Arrays.asList("Case Reports");

		ProcessingStatus expectedStatus1 = new ProcessingStatus(pmid1);
		expectedStatus1.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		expectedStatus1.setYearPublished(expectedYear1);
		for (String pubType : expectedPublicationTypes1) {
			expectedStatus1.addPublicationType(pubType);
		}
		ProcessingStatus expectedStatus2 = new ProcessingStatus(pmid2);
		expectedStatus2.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		expectedStatus2.setYearPublished(expectedYear2);
		for (String pubType : expectedPublicationTypes2) {
			expectedStatus2.addPublicationType(pubType);
		}
		ProcessingStatus expectedStatus3 = new ProcessingStatus(pmid3);
		expectedStatus3.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		expectedStatus3.setYearPublished(expectedYear3);
		for (String pubType : expectedPublicationTypes3) {
			expectedStatus3.addPublicationType(pubType);
		}

		PAssert.that(outputStatus).containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3);

		pipeline.run();

	}

}
