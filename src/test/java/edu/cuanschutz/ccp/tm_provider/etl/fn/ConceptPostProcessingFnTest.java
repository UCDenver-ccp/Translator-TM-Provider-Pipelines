package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ConceptPostProcessingFnTest {

	@Test
	public void testPromoteAnnots() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation annot1 = factory.createAnnotation(25, 35, "some text", "PR:00000000022");
		TextAnnotation annot2 = factory.createAnnotation(25, 35, "some text", "PR:00000000000");
		TextAnnotation annot3 = factory.createAnnotation(0, 5, "some text", "PR:00000000025");
		TextAnnotation annot4 = factory.createAnnotation(0, 5, "some text", "PR:00000000020");

		Set<TextAnnotation> annots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4));

		Map<String, String> promotionMap = new HashMap<String, String>();
		promotionMap.put("PR:00000000025", "PR:00000000020");

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn.promotePrAnnots(annots, promotionMap);

		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot4));

		assertEquals(expectedOutputAnnots.size(), outputAnnots.size());
		assertEquals(expectedOutputAnnots, outputAnnots);
	}

	@Test
	public void testConvertExtensionToObo() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation annot1 = factory.createAnnotation(25, 35, "some text", "PR_EXT:00000000022");
		TextAnnotation annot2 = factory.createAnnotation(25, 35, "some text", "PR:some_extension_cls");
		TextAnnotation annot3 = factory.createAnnotation(0, 5, "some text", "PR:00000000025");
		TextAnnotation annot4 = factory.createAnnotation(0, 5, "some text", "PR:00000000020");

		Set<TextAnnotation> annots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4));
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		extensionToOboMap.put("PR_EXT:00000000022", CollectionsUtil.createSet("PR:00000000022"));
		extensionToOboMap.put("PR:some_extension_cls", CollectionsUtil.createSet("PR:00000000123", "PR:00000000456"));

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn.convertExtensionToObo(annots, extensionToOboMap);

		TextAnnotation annot1Updated = factory.createAnnotation(25, 35, "some text", "PR:00000000022");
		TextAnnotation annot2aUpdated = factory.createAnnotation(25, 35, "some text", "PR:00000000123");
		TextAnnotation annot2bUpdated = factory.createAnnotation(25, 35, "some text", "PR:00000000456");

		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(annot1Updated, annot2aUpdated, annot2bUpdated, annot3, annot4));

		assertEquals(expectedOutputAnnots.size(), outputAnnots.size());
		assertEquals(expectedOutputAnnots, outputAnnots);

	}

	@Test
	public void testPrefer() {

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put("PR:000002012", CollectionsUtil.createSet("PR:000000008"));
		ancestorMap.put("PR:000000046", CollectionsUtil.createSet("PR:000000008"));
		ancestorMap.put("PR:000000101", CollectionsUtil.createSet("PR:000000008", "PR:000000123"));
		ancestorMap.put("PR:000000285",
				CollectionsUtil.createSet("PR:000000008", "PR:000000101", "PR:000000046", "PR:000000123"));
		ancestorMap.put("PR:000000286",
				CollectionsUtil.createSet("PR:000000008", "PR:000000101", "PR:000000046", "PR:000000123"));
		ancestorMap.put("PR:000000552", CollectionsUtil.createSet("PR:000000008", "PR:000000101", "PR:000000046",
				"PR:000000123", "PR:000000286"));
		ancestorMap.put("PR:000002517", CollectionsUtil.createSet("PR:000000008", "PR:000000101", "PR:000000046",
				"PR:000000123", "PR:000000286"));

		assertEquals("single id should return itself", CollectionsUtil.createSet("PR:000000101"),
				ConceptPostProcessingFn.prefer(CollectionsUtil.createSet("PR:000000101"), ancestorMap));
		assertEquals("single id should return itself", CollectionsUtil.createSet("PR:000000286"),
				ConceptPostProcessingFn.prefer(CollectionsUtil.createSet("PR:000000286"), ancestorMap));

		assertEquals("PR:000000101 is the most general concept so it should be returned",
				CollectionsUtil.createSet("PR:000000101"),
				ConceptPostProcessingFn.prefer(CollectionsUtil.createSet("PR:000002517", "PR:000000552", "PR:000000285",
						"PR:000000101", "PR:000000286"), ancestorMap));

		assertEquals(
				"PR:000000101 is the most general concept so it should be returned. "
						+ "PR:000002012 sits by itself (no children) so it should also be returned.",
				CollectionsUtil.createSet("PR:000000101", "PR:000002012"),
				ConceptPostProcessingFn.prefer(CollectionsUtil.createSet("PR:000002517", "PR:000000552", "PR:000000285",
						"PR:000000101", "PR:000000286", "PR:000002012"), ancestorMap));

	}

	@Test
	public void testPromoteNcbiTaxonAnnots() {

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put("NCBITaxon:000002012", CollectionsUtil.createSet("NCBITaxon:000000008"));
		ancestorMap.put("NCBITaxon:000000046", CollectionsUtil.createSet("NCBITaxon:000000008"));
		ancestorMap.put("NCBITaxon:000000101", CollectionsUtil.createSet("NCBITaxon:000000008", "NCBITaxon:000000123"));
		ancestorMap.put("NCBITaxon:000000285", CollectionsUtil.createSet("NCBITaxon:000000008", "NCBITaxon:000000101",
				"NCBITaxon:000000046", "NCBITaxon:000000123"));
		ancestorMap.put("NCBITaxon:000000286", CollectionsUtil.createSet("NCBITaxon:000000008", "NCBITaxon:000000101",
				"NCBITaxon:000000046", "NCBITaxon:000000123"));
		ancestorMap.put("NCBITaxon:000000552", CollectionsUtil.createSet("NCBITaxon:000000008", "NCBITaxon:000000101",
				"NCBITaxon:000000046", "NCBITaxon:000000123", "NCBITaxon:000000286"));
		ancestorMap.put("NCBITaxon:000002517", CollectionsUtil.createSet("NCBITaxon:000000008", "NCBITaxon:000000101",
				"NCBITaxon:000000046", "NCBITaxon:000000123", "NCBITaxon:000000286"));

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation taxonAnnot1 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:000000286");
		TextAnnotation taxonAnnot2 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:000002517");
		TextAnnotation clAnnot = factory.createAnnotation(0, 5, "annot", "CL:0000000");
		TextAnnotation prAnnot = factory.createAnnotation(10, 15, "annot", "PR:000112345");
		TextAnnotation taxonAnnot3 = factory.createAnnotation(20, 25, "annot", "NCBITaxon:000000285");

		Set<TextAnnotation> input = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot2, taxonAnnot3, clAnnot, prAnnot);

		Set<TextAnnotation> output = ConceptPostProcessingFn.promoteNcbiTaxonAnnots(input, ancestorMap);

		Set<TextAnnotation> expectedOutput = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot3, clAnnot, prAnnot);

		assertEquals(expectedOutput, output);

	}

	@Test
	public void testExcludeNcbiTaxonAnnots() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation taxonAnnot1 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:000000286");
		TextAnnotation taxonAnnot2 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:169495");
		TextAnnotation clAnnot = factory.createAnnotation(0, 5, "annot", "CL:0000000");
		TextAnnotation prAnnot = factory.createAnnotation(10, 15, "annot", "PR:000112345");
		TextAnnotation taxonAnnot3 = factory.createAnnotation(20, 25, "annot", "NCBITaxon:000000285");
		Set<TextAnnotation> input = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot2, taxonAnnot3, clAnnot, prAnnot);

		Set<TextAnnotation> output = ConceptPostProcessingFn.excludeSelectNcbiTaxonAnnots(input);

		Set<TextAnnotation> expectedOutput = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot3, clAnnot, prAnnot);

		assertEquals(expectedOutput, output);
	}
	
	
	@Test
	public void testRemoveStopwords() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation taxonAnnot1 = factory.createAnnotation(0, 3, "Was", "NCBITaxon:000000286");
		TextAnnotation taxonAnnot2 = factory.createAnnotation(0, 3, "was", "NCBITaxon:169495");
		TextAnnotation clAnnot = factory.createAnnotation(0, 5, "annot", "CL:0000000");
		TextAnnotation prAnnot = factory.createAnnotation(17, 19, "be", "MONDO:0001234");
		TextAnnotation taxonAnnot3 = factory.createAnnotation(20, 25, "annot", "NCBITaxon:000000285");
		Set<TextAnnotation> input = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot2, taxonAnnot3, clAnnot, prAnnot);

		Set<TextAnnotation> output = ConceptPostProcessingFn.removeNcbiStopWords(input);

		Set<TextAnnotation> expectedOutput = CollectionsUtil.createSet( taxonAnnot3, clAnnot);

		assertEquals(expectedOutput, output);
	}

}
