package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.ConceptPair;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class ConceptCooccurrenceMetricsPipelineTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testCountTotalDocuments() {

		Map<String, Set<String>> input = new HashMap<String, Set<String>>();

		input.put("PMID:1", CollectionsUtil.createSet("AA:1", "AA:2", "AA:3", "AA:4", "ZZ:1", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:2", CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "ZZ:3", "ZZ:4"));
		input.put("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4"));

		PCollection<KV<String, Set<String>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), SetCoder.of(StringUtf8Coder.of()))));

		PCollection<Long> totalDocCount = ConceptCooccurrenceMetricsPipeline.countTotalDocuments(pipeline,
				CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection);

		PAssert.that(totalDocCount).containsInAnyOrder(4l);

		pipeline.run();
	}

	@Test
	public void testCountTotalConcepts() {

		Map<String, Long> input = new HashMap<String, Long>();

		input.put("AA:1", 1l);
		input.put("AA:2", 2l);
		input.put("AA:3", 3l);
		input.put("AA:4", 4l);
		input.put("ZZ:1", 1l);
		input.put("ZZ:2", 2l);
		input.put("ZZ:3", 3l);
		input.put("ZZ:4", 4l);

		PCollection<KV<String, Long>> conceptIdToCounts = pipeline
				.apply(Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())));

		final PCollectionView<Long> totalDocCount = ConceptCooccurrenceMetricsPipeline.countTotalConcepts(pipeline,
				CooccurLevel.DOCUMENT, conceptIdToCounts);

		PCollection<Long> output = getPCollectionViewValue(pipeline, totalDocCount);
		PAssert.that(output).containsInAnyOrder(20l);

		pipeline.run();
	}

	private static PCollection<Long> getPCollectionViewValue(TestPipeline pipeline,
			PCollectionView<Long> totalDocCount) {
		return pipeline.apply("Create input", Create.of(1)).apply(ParDo.of(new DoFn<Integer, Long>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				long count = c.sideInput(totalDocCount);
				c.output(count);
			}
		}).withSideInputs(totalDocCount));

	}

	@Test
	public void testAddAncestorConceptIds() {
		Map<String, Set<String>> input = new HashMap<String, Set<String>>();

		input.put("PMID:1", CollectionsUtil.createSet("AA:1", "AA:2", "AA:3", "AA:4", "ZZ:1", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:2", CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "ZZ:3", "ZZ:4"));
		input.put("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4"));

		PCollection<KV<String, Set<String>>> textIdToConceptIdCollection = pipeline.apply("input",
				Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), SetCoder.of(StringUtf8Coder.of()))));

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put("AA:1", new HashSet<String>(Arrays.asList("AA:0", "AA:00")));
		ancestorMap.put("ZZ:3", new HashSet<String>(Arrays.asList("BB:0", "BB:00")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		PCollection<KV<String, Set<String>>> output = ConceptCooccurrenceMetricsPipeline.addAncestorConceptIds(pipeline,
				CooccurLevel.DOCUMENT, textIdToConceptIdCollection, ancestorMapView);

		List<KV<String, Set<String>>> expectedOutput = new ArrayList<KV<String, Set<String>>>();

		expectedOutput.add(KV.of("PMID:1", CollectionsUtil.createSet("AA:0", "AA:00", "AA:1", "AA:2", "AA:3", "AA:4",
				"ZZ:1", "ZZ:2", "BB:0", "BB:00", "ZZ:3", "ZZ:4")));
		expectedOutput.add(KV.of("PMID:2",
				CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "BB:0", "BB:00", "ZZ:3", "ZZ:4")));
		expectedOutput.add(KV.of("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "BB:0", "BB:00", "ZZ:3", "ZZ:4")));
		expectedOutput.add(KV.of("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4")));

		PAssert.that(output).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

	@Test
	public void testCountConceptObservations() {

		Map<String, Set<String>> input = new HashMap<String, Set<String>>();

		input.put("PMID:1", CollectionsUtil.createSet("AA:1", "AA:2", "AA:3", "AA:4", "ZZ:1", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:2", CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "ZZ:3", "ZZ:4"));
		input.put("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4"));

		PCollection<KV<String, Set<String>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), SetCoder.of(StringUtf8Coder.of()))));

		PCollection<KV<String, Long>> conceptCounts = ConceptCooccurrenceMetricsPipeline
				.countConceptObservations(pipeline, CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection);

		Set<KV<String, Long>> expectedConceptCounts = new HashSet<KV<String, Long>>();
		expectedConceptCounts.add(KV.of("AA:1", 1l));
		expectedConceptCounts.add(KV.of("AA:2", 2l));
		expectedConceptCounts.add(KV.of("AA:3", 3l));
		expectedConceptCounts.add(KV.of("AA:4", 4l));
		expectedConceptCounts.add(KV.of("ZZ:1", 1l));
		expectedConceptCounts.add(KV.of("ZZ:2", 2l));
		expectedConceptCounts.add(KV.of("ZZ:3", 3l));
		expectedConceptCounts.add(KV.of("ZZ:4", 4l));
		PAssert.that(conceptCounts).containsInAnyOrder(expectedConceptCounts);

		pipeline.run();
	}

	@Test
	public void testComputeConceptPairs() {
		Map<String, Set<String>> input = new HashMap<String, Set<String>>();

		input.put("PMID:1", CollectionsUtil.createSet("AA:1", "AA:2", "AA:3", "AA:4", "ZZ:1", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:2", CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "ZZ:3", "ZZ:4"));
		input.put("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4"));

		PCollection<KV<String, Set<String>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), SetCoder.of(StringUtf8Coder.of()))));

		PCollection<KV<String, Set<String>>> pairToDocIds = ConceptCooccurrenceMetricsPipeline
				.computeConceptPairs(pipeline, CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection);

		Set<KV<String, Set<String>>> expectedOutput = new HashSet<KV<String, Set<String>>>();
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3")));
		
		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3")));
		
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1","PMID:2","PMID:3", "PMID:4")));
		
		PAssert.that(pairToDocIds).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

}
