package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
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
	public void testRemoveIdToTextExclusionPairs() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation annot1 = factory.createAnnotation(0, 8, "neuronal", "CL:0000540");
		TextAnnotation annot2 = factory.createAnnotation(23, 29, "neuron", "CL:0000540");
		TextAnnotation annot3 = factory.createAnnotation(38, 47, "centrally", "UBERON:0012131");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3));

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn.removeIdToTextExclusionPairs(inputAnnots);
		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot2));

		assertEquals("annot2 is the only on that should remain.", expectedOutputAnnots, outputAnnots);

	}

	@Test
	public void testRemoveSpuriousMatches() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");

		Map<String, Set<String>> idToOgerDictEntriesMap = new HashMap<String, Set<String>>();
		idToOgerDictEntriesMap.put("PR:O74957",
				new HashSet<String>(Arrays.asList("Spom972h-ago1", "PAZ Piwi domain protein ago1")));

		idToOgerDictEntriesMap.put("PR:000012547", new HashSet<String>(Arrays.asList("Per1")));

		TextAnnotation legitAnnot1 = factory.createAnnotation(0, 28, "PAZ Piwi domain protein ago1", "PR:O74957");
		TextAnnotation legitAnnot2 = factory.createAnnotation(0, 30, "PAZ Piwi domain protein (ago1)", "PR:O74957");
		TextAnnotation spuriousAnnot1 = factory.createAnnotation(0, 6, "ago [1", "PR:O74957");

		Set<TextAnnotation> allAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, spuriousAnnot1));
		Set<TextAnnotation> updatedAnnotations = ConceptPostProcessingFn.removeSpuriousMatches(allAnnots,
				idToOgerDictEntriesMap);
		Set<TextAnnotation> expectedUpdatedAnnotations = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2));
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

		TextAnnotation spuriousAnnot2 = factory.createAnnotation(0, 4, "per", "PR:000012547");

		allAnnots = new HashSet<TextAnnotation>(Arrays.asList(spuriousAnnot2));
		updatedAnnotations = ConceptPostProcessingFn.removeSpuriousMatches(allAnnots, idToOgerDictEntriesMap);
		expectedUpdatedAnnotations = new HashSet<TextAnnotation>(Arrays.asList());
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

		TextAnnotation spuriousAnnot3 = factory.createAnnotation(0, 4, "12.3", "PR:000012547");

		allAnnots = new HashSet<TextAnnotation>(Arrays.asList(spuriousAnnot3));
		updatedAnnotations = ConceptPostProcessingFn.removeSpuriousMatches(allAnnots, idToOgerDictEntriesMap);
		expectedUpdatedAnnotations = new HashSet<TextAnnotation>(Arrays.asList());
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

	}

	@Test
	public void testRemoveSpuriousMatches_UnexpectedGoCcExclusions() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");

		Map<String, Set<String>> idToOgerDictEntriesMap = new HashMap<String, Set<String>>();
		idToOgerDictEntriesMap.put("GO:0030424", new HashSet<String>(Arrays.asList("axon")));
		idToOgerDictEntriesMap.put("GO:0005764", new HashSet<String>(Arrays.asList("lysosome")));
		idToOgerDictEntriesMap.put("GO:0005737", new HashSet<String>(Arrays.asList("cytoplasm")));

		TextAnnotation legitAnnot1 = factory.createAnnotation(0, 5, "axons", "GO:0030424");
		TextAnnotation legitAnnot2 = factory.createAnnotation(0, 9, "lysosomal", "GO:0005764");
		TextAnnotation legitAnnot3 = factory.createAnnotation(0, 9, "cytoplasmic", "GO:0005737");

		Set<TextAnnotation> allAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, legitAnnot3));
		Set<TextAnnotation> updatedAnnotations = ConceptPostProcessingFn.removeSpuriousMatches(allAnnots,
				idToOgerDictEntriesMap);
		Set<TextAnnotation> expectedUpdatedAnnotations = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, legitAnnot3));
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);
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

//	@Test
//	public void testExcludeNcbiTaxonAnnots() {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
//		TextAnnotation taxonAnnot1 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:000000286");
//		TextAnnotation taxonAnnot2 = factory.createAnnotation(0, 5, "annot", "NCBITaxon:169495");
//		TextAnnotation clAnnot = factory.createAnnotation(0, 5, "annot", "CL:0000000");
//		TextAnnotation prAnnot = factory.createAnnotation(10, 15, "annot", "PR:000112345");
//		TextAnnotation taxonAnnot3 = factory.createAnnotation(20, 25, "annot", "NCBITaxon:000000285");
//		Set<TextAnnotation> input = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot2, taxonAnnot3, clAnnot, prAnnot);
//
//		Set<TextAnnotation> output = ConceptPostProcessingFn.excludeSelectNcbiTaxonAnnots(input);
//
//		Set<TextAnnotation> expectedOutput = CollectionsUtil.createSet(taxonAnnot1, taxonAnnot3, clAnnot, prAnnot);
//
//		assertEquals(expectedOutput, output);
//	}

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

		Set<TextAnnotation> expectedOutput = CollectionsUtil.createSet(taxonAnnot3, clAnnot);

		assertEquals(expectedOutput, output);
	}

	@Test
	public void testRemoveAllAbbreviationShortFormAnnots() throws IOException {

		String sentence1 = "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine.";

		// 01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
		String sentence2 = "The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD.";

		String documentText = sentence1 + " " + sentence2;

		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		sentenceToSpanMap.put(sentence1, new Span(0, 134));
		sentenceToSpanMap.put(sentence2, new Span(0 + 135, 132 + 135));

		// @formatter:off
		String abbreviationsBionlp = 
				"T1\tlong_form 0 24\tAldehyde dehydrogenase 1\n" +
		        "T2\tshort_form 26 31\tALDH1\n" + 
				"T3\tlong_form 67 86\tParkinson's disease\n" +
		        "T4\tshort_form 88 90\tPD\n" +
				"T5\tlong_form 151 177\taldehyde dehydrogenase 1A3\n" + 
		        "T6\tshort_form 179 186\tALDH1A3\n" + 
				"R1\thas_short_form Arg1:T3 Arg2:T4\n" +
				"R2\thas_short_form Arg1:T1 Arg2:T2\n" +
				"R3\thas_short_form Arg1:T5 Arg2:T6\n";
		// @formatter:on

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument abbrevDoc = bionlpReader.readDocument("PMID:12345", "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation aldh1LongAnnot = factory.createAnnotation(0, 24, "Aldehyde dehydrogenase 1", "PR:1");
		TextAnnotation aldh1ShortAnnot = factory.createAnnotation(26, 31, "ALDH1", "PR:1");
		TextAnnotation spuriousAldh1ShortAnnot = factory.createAnnotation(26, 31, "ALDH1", "PR:111");

		TextAnnotation pdLongAnnot = factory.createAnnotation(67, 86, "Parkinson's disease", "MONDO:1");
		TextAnnotation pdShortAnnot = factory.createAnnotation(88, 90, "PD", "MONDO:1");

		TextAnnotation aldh1a3LongAnnot = factory.createAnnotation(151, 177, "aldehyde dehydrogenase 1A3", "PR:2");
		TextAnnotation aldhLongAnnot = factory.createAnnotation(151, 173, "aldehyde dehydrogenase", "PR:3");
		TextAnnotation aldh1a3ShortAnnot = factory.createAnnotation(179, 186, "ALDH1A3", "PR:2");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(aldh1LongAnnot, aldh1ShortAnnot, spuriousAldh1ShortAnnot, pdLongAnnot, pdShortAnnot,
						aldh1a3LongAnnot, aldhLongAnnot, aldh1a3ShortAnnot));
		Set<TextAnnotation> updatedAnnots = ConceptPostProcessingFn.removeAllAbbreviationShortFormAnnots(inputAnnots,
				abbrevDoc.getAnnotations());

		Set<TextAnnotation> expectedUpdatedAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(aldh1LongAnnot, pdLongAnnot, aldh1a3LongAnnot, aldhLongAnnot));

		assertEquals(expectedUpdatedAnnots, updatedAnnots);
	}

	@Test
	public void testPropagateAbbreviationLongFormConceptsToShortFormMentions() throws IOException {
		String sentence1 = "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine.";

		// 01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
		String sentence2 = "The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD.";

		String documentText = sentence1 + " " + sentence2;

		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		sentenceToSpanMap.put(sentence1, new Span(0, 134));
		sentenceToSpanMap.put(sentence2, new Span(0 + 135, 132 + 135));

		// @formatter:off
		String abbreviationsBionlp = 
				"T1\tlong_form 0 24\tAldehyde dehydrogenase 1\n" +
		        "T2\tshort_form 26 31\tALDH1\n" + 
				"T3\tlong_form 67 86\tParkinson's disease\n" +
		        "T4\tshort_form 88 90\tPD\n" +
				"T5\tlong_form 151 177\taldehyde dehydrogenase 1A3\n" + 
		        "T6\tshort_form 179 186\tALDH1A3\n" + 
				"R1\thas_short_form Arg1:T3 Arg2:T4\n" +
				"R2\thas_short_form Arg1:T1 Arg2:T2\n" +
				"R3\thas_short_form Arg1:T5 Arg2:T6\n";
		// @formatter:on

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument abbrevDoc = bionlpReader.readDocument("PMID:12345", "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation aldh1LongAnnot = factory.createAnnotation(0, 24, "Aldehyde dehydrogenase 1", "PR:1");
		TextAnnotation aldh1ShortAnnot = factory.createAnnotation(26, 31, "ALDH1", "PR:1");
		TextAnnotation newAldh1ShortAnnot = factory.createAnnotation(298, 303, "ALDH1", "PR:1");

		TextAnnotation pdLongAnnot = factory.createAnnotation(67, 86, "Parkinson's disease", "MONDO:1");
		TextAnnotation pdShortAnnot = factory.createAnnotation(88, 90, "PD", "MONDO:1");
		TextAnnotation newPdShortAnnot = factory.createAnnotation(335, 337, "PD", "MONDO:1");

		TextAnnotation aldh1a3LongAnnot = factory.createAnnotation(151, 177, "aldehyde dehydrogenase 1A3", "PR:2");
		TextAnnotation aldhLongAnnot = factory.createAnnotation(151, 173, "aldehyde dehydrogenase", "PR:3");
		TextAnnotation aldh1a3ShortAnnot = factory.createAnnotation(179, 186, "ALDH1A3", "PR:2");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(aldh1LongAnnot, pdLongAnnot, aldh1a3LongAnnot, aldhLongAnnot));

		Set<TextAnnotation> updatedAnnots = ConceptPostProcessingFn
				.propagateAbbreviationLongFormConceptsToShortFormMentions(inputAnnots, abbrevDoc.getAnnotations(),
						"PMID:12345", documentText);

		// the short form annots should be populated, including two new annots at the
		// end of the second sentence; the ALDH1A3 annot should be correct since the
		// code looks for the longest overlap of a long-form annot
		Set<TextAnnotation> expectedUpdatedAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(aldh1LongAnnot, aldh1ShortAnnot, newAldh1ShortAnnot, pdLongAnnot, pdShortAnnot,
						newPdShortAnnot, aldh1a3LongAnnot, aldhLongAnnot, aldh1a3ShortAnnot));

		assertEquals(expectedUpdatedAnnots, updatedAnnots);
	}
}
