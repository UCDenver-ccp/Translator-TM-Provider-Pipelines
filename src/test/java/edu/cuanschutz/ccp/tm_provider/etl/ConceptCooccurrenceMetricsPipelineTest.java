package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.ConceptCooccurrenceMetricsPipeline.ConceptId;
import edu.cuanschutz.ccp.tm_provider.etl.ConceptCooccurrenceMetricsPipeline.CooccurrencePublication;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.ConceptPair;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class ConceptCooccurrenceMetricsPipelineTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testRounding() {
		double d = 2.0 / 3.0;
		System.out.println(d);

		BigDecimal a = new BigDecimal(d).setScale(8, BigDecimal.ROUND_HALF_UP);
		System.out.println(a);

		System.out.println(a.doubleValue());
	}

	@Test
	public void testCountTotalDocuments() {
		Map<String, Set<ConceptId>> input = new HashMap<String, Set<ConceptId>>();

		input.put("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:1", true), new ConceptId("AA:2", true),
						new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:1", true),
						new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true),
						new ConceptId("ZZ:4", true)));
		input.put("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:4", true), new ConceptId("ZZ:4", true)));

		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(
						KvCoder.of(StringUtf8Coder.of(), SetCoder.of(SerializableCoder.of(ConceptId.class)))));

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
		// note that BB:00 should get excluded when ancestor concept IDs are added since
		// it has a different prefix than the ZZ concepts
		ancestorMap.put("ZZ:3", new HashSet<String>(Arrays.asList("ZZ:0", "BB:00")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		boolean addAncestors = true;
		PCollection<KV<String, Set<ConceptId>>> output = ConceptCooccurrenceMetricsPipeline.addAncestorConceptIds(
				pipeline, CooccurLevel.DOCUMENT, textIdToConceptIdCollection, addAncestors, ancestorMapView);

		List<KV<String, Set<ConceptId>>> expectedOutput = new ArrayList<KV<String, Set<ConceptId>>>();

		expectedOutput.add(KV.of("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:0", false), new ConceptId("AA:00", false),
						new ConceptId("AA:1", true), new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:1", true), new ConceptId("ZZ:2", true),
						new ConceptId("ZZ:0", false), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput.add(KV.of("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:0", false),
						new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput
				.add(KV.of("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
						new ConceptId("ZZ:0", false), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput.add(
				KV.of("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:4", true), new ConceptId("ZZ:4", true))));

		PAssert.that(output).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

	@Test
	public void testAddAncestorFalseConceptIds() {

		Map<String, Set<String>> input = new HashMap<String, Set<String>>();

		input.put("PMID:1", CollectionsUtil.createSet("AA:1", "AA:2", "AA:3", "AA:4", "ZZ:1", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:2", CollectionsUtil.createSet("AA:2", "AA:3", "AA:4", "ZZ:2", "ZZ:3", "ZZ:4"));
		input.put("PMID:3", CollectionsUtil.createSet("AA:3", "AA:4", "ZZ:3", "ZZ:4"));
		input.put("PMID:4", CollectionsUtil.createSet("AA:4", "ZZ:4"));

		PCollection<KV<String, Set<String>>> textIdToConceptIdCollection = pipeline.apply("input",
				Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), SetCoder.of(StringUtf8Coder.of()))));

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put("AA:1", new HashSet<String>(Arrays.asList("AA:0", "AA:00")));
		// note that BB:00 should get excluded when ancestor concept IDs are added since
		// it has a different prefix than the ZZ concepts
		ancestorMap.put("ZZ:3", new HashSet<String>(Arrays.asList("ZZ:0", "BB:00")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		boolean addAncestors = false;
		PCollection<KV<String, Set<ConceptId>>> output = ConceptCooccurrenceMetricsPipeline.addAncestorConceptIds(
				pipeline, CooccurLevel.DOCUMENT, textIdToConceptIdCollection, addAncestors, ancestorMapView);

		List<KV<String, Set<ConceptId>>> expectedOutput = new ArrayList<KV<String, Set<ConceptId>>>();

		expectedOutput.add(KV.of("PMID:1", CollectionsUtil.createSet(// new ConceptId("AA:0", false), new
																		// ConceptId("AA:00", false),
				new ConceptId("AA:1", true), new ConceptId("AA:2", true), new ConceptId("AA:3", true),
				new ConceptId("AA:4", true), new ConceptId("ZZ:1", true), new ConceptId("ZZ:2", true),
//						new ConceptId("ZZ:0", false), 
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput.add(KV.of("PMID:2", CollectionsUtil.createSet(new ConceptId("AA:2", true),
				new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:2", true),
//						new ConceptId("ZZ:0", false),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput
				.add(KV.of("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
//						new ConceptId("ZZ:0", false), 
						new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true))));
		expectedOutput.add(
				KV.of("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:4", true), new ConceptId("ZZ:4", true))));

		PAssert.that(output).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

	@Test
	public void testCountConceptObservations() {
		Map<String, Set<ConceptId>> input = new HashMap<String, Set<ConceptId>>();

		input.put("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:1", true), new ConceptId("AA:2", true),
						new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:1", true),
						new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true),
						new ConceptId("ZZ:4", true)));
		input.put("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:4", true), new ConceptId("ZZ:4", true)));

		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(
						KvCoder.of(StringUtf8Coder.of(), SetCoder.of(SerializableCoder.of(ConceptId.class)))));

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
		Map<String, Set<ConceptId>> input = new HashMap<String, Set<ConceptId>>();

		input.put("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:1", true), new ConceptId("AA:2", true),
						new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:1", true),
						new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true),
						new ConceptId("ZZ:4", true)));
		input.put("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:4", true), new ConceptId("ZZ:4", true)));

		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(
						KvCoder.of(StringUtf8Coder.of(), SetCoder.of(SerializableCoder.of(ConceptId.class)))));

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		// add unrelated ancestors so that the Coder can be inferred (this avoids an
		// IllegalArgumentException)
		ancestorMap.put("NULL:1", new HashSet<String>(Arrays.asList("NULL:0")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		Set<String> conceptPrefixesToInclude = new HashSet<String>(Arrays.asList("AA", "ZZ"));
		PCollectionTuple pairAndPubs = ConceptCooccurrenceMetricsPipeline.computeConceptPairs(pipeline,
				CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection, ancestorMapView,
				conceptPrefixesToInclude);

		PCollection<KV<String, String>> conceptPairIdToTextId = pairAndPubs
				.get(ConceptCooccurrenceMetricsPipeline.PAIR_KEY_TO_DOC_ID_TAG);
		PCollection<KV<String, Set<String>>> pairToDocIds = ConceptCooccurrenceMetricsPipeline
				.groupByPairId(CooccurLevel.DOCUMENT, conceptPairIdToTextId);

		Set<KV<String, Set<String>>> expectedOutput = new HashSet<KV<String, Set<String>>>();
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "AA:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:3").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "AA:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));

		expectedOutput
				.add(KV.of(new ConceptPair("ZZ:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("ZZ:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("ZZ:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:3").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:3", "ZZ:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));

		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:2", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:2").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:3").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:3", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:2").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:3").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
		expectedOutput
				.add(KV.of(new ConceptPair("AA:4", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:2").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:3").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:4").toReproducibleKey(),
				CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4")));

		PAssert.that(pairToDocIds).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

	@Test
	public void testComputeConceptPairsExcludingAncestorPairs() {
		Map<String, Set<ConceptId>> input = new HashMap<String, Set<ConceptId>>();

		input.put("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:1", true), new ConceptId("AA:2", true),
						new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:1", true),
						new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true),
						new ConceptId("ZZ:4", true)));
		input.put("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:3", false), new ConceptId("AA:4", true),
				new ConceptId("ZZ:4", true)));

		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(
						KvCoder.of(StringUtf8Coder.of(), SetCoder.of(SerializableCoder.of(ConceptId.class)))));

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		// add unrelated ancestors so that the Coder can be inferred (this avoids an
		// IllegalArgumentException)
		ancestorMap.put("AA:4", new HashSet<String>(Arrays.asList("AA:3")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		Set<String> conceptPrefixesToInclude = new HashSet<String>(Arrays.asList("AA", "ZZ"));
		PCollectionTuple pairAndPubs = ConceptCooccurrenceMetricsPipeline.computeConceptPairs(pipeline,
				CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection, ancestorMapView,
				conceptPrefixesToInclude);

		PCollection<KV<String, String>> conceptPairIdToTextId = pairAndPubs
				.get(ConceptCooccurrenceMetricsPipeline.PAIR_KEY_TO_DOC_ID_TAG);
		PCollection<KV<String, Set<String>>> pairToDocIds = ConceptCooccurrenceMetricsPipeline
				.groupByPairId(CooccurLevel.DOCUMENT, conceptPairIdToTextId);

		// @formatter:off
		Set<KV<String, Set<String>>> expectedOutput = new HashSet<KV<String, Set<String>>>();
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		// this pair is excluded b/c of the ancestor relationship between AA:4 and AA:3
		// expectedOutput.add(KV.of(new ConceptPair("AA:3", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));

		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("ZZ:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));

		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
		// the AA:3, ZZ:4 pair in  PMID:4 is due to the ancestor relationship between AA:4 and AA:3
		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4")));
		// @formatter:on
		PAssert.that(pairToDocIds).containsInAnyOrder(expectedOutput);

		PCollection<KV<String, CooccurrencePublication>> pairIdToPublication = pairAndPubs
				.get(ConceptCooccurrenceMetricsPipeline.PAIR_PUBLICATIONS_TAG);

		PCollection<CooccurrencePublication> pubs = ConceptCooccurrenceMetricsPipeline
				.limitPublicationsByPairId(CooccurLevel.DOCUMENT, pairIdToPublication);

		CooccurrencePublication[] expectedPubs = new CooccurrencePublication[] {

				new CooccurrencePublication(new ConceptPair("AA:1", "AA:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:1", "AA:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:1", "AA:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "AA:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "AA:3"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:2", "AA:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "AA:4"), "PMID:2"),

				new CooccurrencePublication(new ConceptPair("ZZ:1", "ZZ:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:1", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:1", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:2", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:2", "ZZ:3"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("ZZ:2", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:2", "ZZ:4"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("ZZ:3", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("ZZ:3", "ZZ:4"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("ZZ:3", "ZZ:4"), "PMID:3"),

				new CooccurrencePublication(new ConceptPair("AA:1", "ZZ:1"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:1", "ZZ:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:1", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:1", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:1"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:2"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:3"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:2", "ZZ:4"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:1"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:2"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:3"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:3"), "PMID:3"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:4"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:4"), "PMID:3"),

//						new CooccurrencePublication(new ConceptPair("AA:3", "ZZ:4"), "PMID:4")

				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:1"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:2"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:2"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:3"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:3"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:3"), "PMID:3"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:4"), "PMID:1"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:4"), "PMID:2"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:4"), "PMID:3"),
				new CooccurrencePublication(new ConceptPair("AA:4", "ZZ:4"), "PMID:4"),

		};

		PAssert.that(pubs).containsInAnyOrder(expectedPubs);

		pipeline.run();
	}

	@Test
	public void testComputeConceptPairsExcludingAncestorPairsExcludeZZ() {
		Map<String, Set<ConceptId>> input = new HashMap<String, Set<ConceptId>>();

		input.put("PMID:1",
				CollectionsUtil.createSet(new ConceptId("AA:1", true), new ConceptId("AA:2", true),
						new ConceptId("AA:3", true), new ConceptId("AA:4", true), new ConceptId("ZZ:1", true),
						new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:2",
				CollectionsUtil.createSet(new ConceptId("AA:2", true), new ConceptId("AA:3", true),
						new ConceptId("AA:4", true), new ConceptId("ZZ:2", true), new ConceptId("ZZ:3", true),
						new ConceptId("ZZ:4", true)));
		input.put("PMID:3", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:3", true), new ConceptId("ZZ:4", true)));
		input.put("PMID:4", CollectionsUtil.createSet(new ConceptId("AA:3", true), new ConceptId("AA:4", true),
				new ConceptId("ZZ:4", true)));

		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = pipeline
				.apply(Create.of(input).withCoder(
						KvCoder.of(StringUtf8Coder.of(), SetCoder.of(SerializableCoder.of(ConceptId.class)))));

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		// add unrelated ancestors so that the Coder can be inferred (this avoids an
		// IllegalArgumentException)
		ancestorMap.put("AA:4", new HashSet<String>(Arrays.asList("AA:3")));

		PCollectionView<Map<String, Set<String>>> ancestorMapView = pipeline
				.apply("ancestor view", Create.of(ancestorMap)).apply(View.asMap());

		Set<String> conceptPrefixesToInclude = new HashSet<String>(Arrays.asList("AA"));
		PCollectionTuple pairAndPubs = ConceptCooccurrenceMetricsPipeline.computeConceptPairs(pipeline,
				CooccurLevel.DOCUMENT, textIdToConceptIdWithAncestorsCollection, ancestorMapView,
				conceptPrefixesToInclude);

		PCollection<KV<String, String>> conceptPairIdToTextId = pairAndPubs
				.get(ConceptCooccurrenceMetricsPipeline.PAIR_KEY_TO_DOC_ID_TAG);
		PCollection<KV<String, Set<String>>> pairToDocIds = ConceptCooccurrenceMetricsPipeline
				.groupByPairId(CooccurLevel.DOCUMENT, conceptPairIdToTextId);

		// @formatter:off
		Set<KV<String, Set<String>>> expectedOutput = new HashSet<KV<String, Set<String>>>();
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:1", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		expectedOutput.add(KV.of(new ConceptPair("AA:2", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
		// this pair is excluded b/c of the ancestor relationship between AA:4 and AA:3
		// expectedOutput.add(KV.of(new ConceptPair("AA:3", "AA:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));

//		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("ZZ:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("ZZ:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("ZZ:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
//
//		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:1", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:2", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
//		// the AA:3, ZZ:4 pair in  PMID:4 is due to the ancestor relationship between AA:4 and AA:3
//		expectedOutput.add(KV.of(new ConceptPair("AA:3", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:1").toReproducibleKey(), CollectionsUtil.createSet("PMID:1")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:2").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:3").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3")));
//		expectedOutput.add(KV.of(new ConceptPair("AA:4", "ZZ:4").toReproducibleKey(), CollectionsUtil.createSet("PMID:1", "PMID:2", "PMID:3", "PMID:4")));
		// @formatter:on
		PAssert.that(pairToDocIds).containsInAnyOrder(expectedOutput);

		pipeline.run();
	}

	@Test
	public void testGetDocumentIdToStore() {

		String titleId = "PMID:208421_title_3593a05b85c4d250540e29ee7c16addf32ab026eb2461005f40234d4fa4623c9";
		String documentId = "PMID:208421";

		String documentIdToStore = ConceptCooccurrenceMetricsPipeline.getDocumentIdToStore(titleId);
		assertEquals(documentId, documentIdToStore);

		documentIdToStore = ConceptCooccurrenceMetricsPipeline.getDocumentIdToStore(documentId);
		assertEquals(documentId, documentIdToStore);

	}

}
