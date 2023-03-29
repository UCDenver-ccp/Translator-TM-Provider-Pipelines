package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Set;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.UpdateIdCollectionPipeline.SetDiffFn;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class UpdateIdCollectionPipelineTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testSetDiffFn() {

		// simulate empty PCollectionView
		PCollectionView<Set<String>> existingCollectionIds = pipeline
				.apply("Create schema view",
						Create.<Set<String>>of(
								CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4", "PMID:5")))
				.apply(View.<Set<String>>asSingleton());

		PCollection<String> potentialNewIds = pipeline
				.apply(Create.of(CollectionsUtil.createList("PMID:3", "PMID:6", "PMID:7")));

		PCollection<String> newIdsToAddToCollection = SetDiffFn.process(existingCollectionIds, potentialNewIds);

		PAssert.that(newIdsToAddToCollection).containsInAnyOrder("PMID:6", "PMID:7");

		pipeline.run();

	}

}
