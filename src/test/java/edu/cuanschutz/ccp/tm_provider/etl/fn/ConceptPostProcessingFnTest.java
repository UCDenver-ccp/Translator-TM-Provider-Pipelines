package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptPostProcessingFn.AugmentedSentence;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptPostProcessingFn.Overlap;
import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultComplexSlotMention;

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
	public void testPropagateRegularAbbreviations() throws IOException {
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

		Set<TextAnnotation> updatedAnnots = ConceptPostProcessingFn.propagateRegularAbbreviations(inputAnnots,
				abbrevDoc.getAnnotations(), "PMID:12345", documentText);

		// the short form annots should be populated, including two new annots at the
		// end of the second sentence; the ALDH1A3 annot should be correct since the
		// code looks for the longest overlap of a long-form annot
		Set<TextAnnotation> expectedUpdatedAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(aldh1LongAnnot, aldh1ShortAnnot, newAldh1ShortAnnot, pdLongAnnot, pdShortAnnot,
						newPdShortAnnot, aldh1a3LongAnnot, aldhLongAnnot, aldh1a3ShortAnnot));

		assertEquals(expectedUpdatedAnnots, updatedAnnots);
	}

	private String getSentence1Text() {
		return "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells.";
	}

	private String getSentence2Text() {
		return "The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD with the help of ES cells, and ES as its own thing.";
	}

	private String getDocumentText() {
		return getSentence1Text() + " " + getSentence2Text();
	}

	private String getAugSent1aText() {
		return "Aldehyde dehydrogenase 1         has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells.";
	}

	private String getAugSent1bText() {
		return "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease      by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells.";
	}

	private String getAugSent1cText() {
		return "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem      cells.";
	}

	private String getAugSent2aText() {
		return "The function of aldehyde dehydrogenase 1A3           in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD with the help of ES cells, and ES as its own thing.";
	}

	private TextDocument getAbbreviationDoc() throws IOException {
		// @formatter:off
		String abbreviationsBionlp = 
				"T1\tlong_form 0 24\tAldehyde dehydrogenase 1\n" +
		        "T2\tshort_form 26 31\tALDH1\n" + 
				"T3\tlong_form 67 86\tParkinson's disease\n" +
		        "T4\tshort_form 88 90\tPD\n" +
		        "T5\tlong_form 161 175\tembryonic stem\n" +
		        "T6\tshort_form 177 179\tES\n" +
				"T7\tlong_form 204 230\taldehyde dehydrogenase 1A3\n" + 
		        "T8\tshort_form 232 239\tALDH1A3\n" + 
				"R1\thas_short_form Arg1:T3 Arg2:T4\n" +
				"R2\thas_short_form Arg1:T1 Arg2:T2\n" +
				"R3\thas_short_form Arg1:T5 Arg2:T6\n" +
				"R4\thas_short_form Arg1:T7 Arg2:T8\n";
		// @formatter:on

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument abbrevDoc = bionlpReader.readDocument(docId, "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(getDocumentText().getBytes()), CharacterEncoding.UTF_8);
		return abbrevDoc;
	}

	private TextAnnotation getAldh1AbbrevAnnot() throws IOException {
		for (TextAnnotation annot : getAbbreviationDoc().getAnnotations()) {
			if (annot.getCoveredText().equals("Aldehyde dehydrogenase 1")) {
				return annot;
			}
		}
		throw new IllegalStateException("Did not find abbrev annot.");
	}

	private TextAnnotation getPdAbbrevAnnot() throws IOException {
		for (TextAnnotation annot : getAbbreviationDoc().getAnnotations()) {
			if (annot.getCoveredText().equals("Parkinson's disease")) {
				return annot;
			}
		}
		throw new IllegalStateException("Did not find abbrev annot.");
	}

	private TextAnnotation getAldh1a3AbbrevAnnot() throws IOException {
		for (TextAnnotation annot : getAbbreviationDoc().getAnnotations()) {
			if (annot.getCoveredText().equals("aldehyde dehydrogenase 1A3")) {
				return annot;
			}
		}
		throw new IllegalStateException("Did not find abbrev annot.");
	}

	private TextAnnotation getEsAbbrevAnnot() throws IOException {
		for (TextAnnotation annot : getAbbreviationDoc().getAnnotations()) {
			if (annot.getCoveredText().equals("embryonic stem")) {
				return annot;
			}
		}
		throw new IllegalStateException("Did not find abbrev annot.");

	}

	private static final String docId = "PMID:12345";
	private static final TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);

	private enum ConceptAnnot {
		ALDH1_LONG_ANNOT, ALDH1_SHORT_ANNOT, PD_LONG_ANNOT, PD_SHORT_ANNOT, ALDH1A3_LONG_ANNOT, ALDH1A3_SHORT_ANNOT,
		ALDH_LONG_ANNOT, ES_LONG_ANNOT, ES_SHORT_ANNOT,

		AUG_1A_ALDH1_LONG_ANNOT, AUG_1B_ALDH1_LONG_ANNOT, AUG_1C_ALDH1_LONG_ANNOT, AUG_1A_PD_LONG_ANNOT,
		AUG_1B_PD_LONG_ANNOT, AUG_1C_PD_LONG_ANNOT, AUG_1A_ES_LONG_ANNOT, AUG_1B_ES_LONG_ANNOT, AUG_1C_ES_LONG_ANNOT,
		AUG_2A_ALDH1A3_LONG_ANNOT, AUG_2A_ALDH_LONG_ANNOT,

		AUG_1C_EMBRYONIC_STEM_CELLS_ANNOT, ORIG_EMBRYONIC_STEM_CELLS_ANNOT, ORIG_ES_CELLS_ANNOT, ORIG_ES_ANNOT
	}

	private TextAnnotation getConceptAnnot(ConceptAnnot ca) throws IOException {
		switch (ca) {
		case ALDH1_LONG_ANNOT:
			return factory.createAnnotation(0, 24, "Aldehyde dehydrogenase 1", "PR:1");
		case ALDH1_SHORT_ANNOT:
			return factory.createAnnotation(26, 31, "ALDH1", "PR:1");
		case PD_LONG_ANNOT:
			return factory.createAnnotation(67, 86, "Parkinson's disease", "MONDO:1");
		case PD_SHORT_ANNOT:
			return factory.createAnnotation(88, 90, "PD", "MONDO:1");
		case ALDH1A3_LONG_ANNOT:
			return factory.createAnnotation(204, 230, "aldehyde dehydrogenase 1A3", "PR:2");
		case ALDH1A3_SHORT_ANNOT:
			return factory.createAnnotation(232, 239, "ALDH1A3", "PR:2");
		case ALDH_LONG_ANNOT:
			return factory.createAnnotation(204, 226, "aldehyde dehydrogenase", "PR:3");
		case ES_LONG_ANNOT:
			return factory.createAnnotation(161, 175, "embryonic stem", "X:1");
		case ES_SHORT_ANNOT:
			return factory.createAnnotation(177, 179, "ES", "X:1");

		// this next group of annotations are those found by OGER in the augmented
		// section of the doc txt
		case AUG_1A_ALDH1_LONG_ANNOT:
			int offset = getAugSent1aAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(0 + offset, 24 + offset, "Aldehyde dehydrogenase 1", "PR:1");
		case AUG_1B_ALDH1_LONG_ANNOT:
			offset = getAugSent1bAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(0 + offset, 24 + offset, "Aldehyde dehydrogenase 1", "PR:1");
		case AUG_1C_ALDH1_LONG_ANNOT:
			offset = getAugSent1cAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(0 + offset, 24 + offset, "Aldehyde dehydrogenase 1", "PR:1");
		case AUG_1A_PD_LONG_ANNOT:
			offset = getAugSent1aAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(67 + offset, 86 + offset, "Parkinson's disease", "MONDO:1");
		case AUG_1B_PD_LONG_ANNOT:
			offset = getAugSent1bAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(67 + offset, 86 + offset, "Parkinson's disease", "MONDO:1");
		case AUG_1C_PD_LONG_ANNOT:
			offset = getAugSent1cAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(67 + offset, 86 + offset, "Parkinson's disease", "MONDO:1");
		case AUG_1A_ES_LONG_ANNOT:
			offset = getAugSent1aAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(161 + offset, 175 + offset, "embryonic stem", "X:1");
		case AUG_1B_ES_LONG_ANNOT:
			offset = getAugSent1bAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(161 + offset, 175 + offset, "embryonic stem", "X:1");
		case AUG_1C_ES_LONG_ANNOT:
			offset = getAugSent1cAnnot().getAnnotationSpanStart();
			return factory.createAnnotation(161 + offset, 175 + offset, "embryonic stem", "X:1");
		case AUG_2A_ALDH1A3_LONG_ANNOT:
			offset = getAugSent2aAnnot().getAnnotationSpanStart() - getSentence1Text().length() - 1;
			return factory.createAnnotation(204 + offset, 230 + offset, "aldehyde dehydrogenase 1A3", "PR:2");
		case AUG_2A_ALDH_LONG_ANNOT:
			offset = getAugSent2aAnnot().getAnnotationSpanStart() - getSentence1Text().length() - 1;
			return factory.createAnnotation(204 + offset, 226 + offset, "aldehyde dehydrogenase", "PR:3");
		case AUG_1C_EMBRYONIC_STEM_CELLS_ANNOT:
			// this is the annotation that gets mapped in the augmented doc text for
			// embryonic stem cells
			String esCellsLongStr = "embryonic stem      cells";
			int start = getDocTextWithAugmentedSection().indexOf(esCellsLongStr);
			return factory.createAnnotation(start, start + esCellsLongStr.length(), esCellsLongStr, "CL:1");

		// this next group of annotations are new annotations created during
		// post-processing/abbreviation handling
		case ORIG_EMBRYONIC_STEM_CELLS_ANNOT:
			// this is the corresponding annotation to the one that got mapped in the
			// augmented doc text
			return factory.createAnnotation(161, 186, "embryonic stem (ES) cells", "CL:1");
		case ORIG_ES_CELLS_ANNOT:
			// this is the ES cells annotation in the original document text
			String esCellsShortStr = "ES cells";
			start = getDocumentText().indexOf(esCellsShortStr);
			return factory.createAnnotation(start, start + esCellsShortStr.length(), esCellsShortStr, "CL:1");
		case ORIG_ES_ANNOT:
			// this is the ES annotation (separate from the ES cells annotation) in the
			// original document text
			String esStr = "ES";
			start = getDocumentText().lastIndexOf(esStr);
			return factory.createAnnotation(start, start + esStr.length(), esStr, "X:1");
		default:
			throw new IllegalArgumentException("Unknown concept annot: " + ca.name());
		}
	}

	/**
	 * @return the set of TextAnnotations that would have been returned from Oger
	 *         (or some other concept recognition system)
	 * @throws IOException
	 */
	private Set<TextAnnotation> getOgerConceptAnnots() throws IOException {
		return new HashSet<TextAnnotation>(Arrays.asList(getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT), getConceptAnnot(ConceptAnnot.ALDH1A3_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.ALDH_LONG_ANNOT), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT),

				getConceptAnnot(ConceptAnnot.AUG_1A_ALDH1_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_1B_ALDH1_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_1C_ALDH1_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_1A_PD_LONG_ANNOT), getConceptAnnot(ConceptAnnot.AUG_1B_PD_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_1C_PD_LONG_ANNOT), getConceptAnnot(ConceptAnnot.AUG_1A_ES_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_1B_ES_LONG_ANNOT), getConceptAnnot(ConceptAnnot.AUG_1C_ES_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_2A_ALDH1A3_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.AUG_2A_ALDH_LONG_ANNOT),

				getConceptAnnot(ConceptAnnot.AUG_1C_EMBRYONIC_STEM_CELLS_ANNOT)));
	}

	/**
	 * @return OGER concepts that overlap with sentence 1
	 * @throws IOException
	 */
	private Set<TextAnnotation> getOgerConceptAnnotsSent1() throws IOException {
		return new HashSet<TextAnnotation>(Arrays.asList(getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT)));
	}

	/**
	 * @return OGER concepts that overlap with sentence 2
	 * @throws IOException
	 */
	private Set<TextAnnotation> getOgerConceptAnnotsSent2() throws IOException {
		return new HashSet<TextAnnotation>(Arrays.asList(getConceptAnnot(ConceptAnnot.ALDH1A3_LONG_ANNOT),
				getConceptAnnot(ConceptAnnot.ALDH_LONG_ANNOT)));
	}

	/**
	 * Ensure that the concepts in each sentence overlap with the correct sentence
	 * 
	 * @throws IOException
	 */
	@Test
	public void testSentenceLevelConceptSets() throws IOException {
		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent1()) {
			assertTrue(conceptAnnot.overlaps(getSentence1Annot()));
		}
		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent2()) {
			assertTrue(conceptAnnot.overlaps(getSentence2Annot()));
		}
	}

	/**
	 * 
	 * <pre>
	 * Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells. The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD with the help of ES cells.
	 * zzzDOCUMENTzENDzzz
	 * AUGSENT	0	0	31
	 * Aldehyde dehydrogenase 1         has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells.
	 * AUGSENT	0	67	90
	 * Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease      by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem (ES) cells.
	 * AUGSENT	0	161	179
	 * Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine and stunting the growth of embryonic stem      cells.
	 * AUGSENT	188	204	239
	 * The function of aldehyde dehydrogenase 1A3           in invasion was assessed by performing transwell assays and animal experiments; it was shown to interact with ALDH1 and provide protection against PD with the help of ES cells.
	 * </pre>
	 * 
	 * @return
	 * @throws IOException
	 */
	private String getDocTextWithAugmentedSection() throws IOException {
		Collection<TextAnnotation> sentenceAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(getSentence1Annot(), getSentence2Annot()));

		String augmentedDocumentText = DocumentTextAugmentationFn.getAugmentedDocumentTextAndSentenceBionlp(
				getDocumentText(), getAbbreviationDoc().getAnnotations(), sentenceAnnots)[0];
		return getDocumentText() + augmentedDocumentText;
	}

	private TextAnnotation getSentence1Annot() {
		String sentence1 = getSentence1Text();
		return factory.createAnnotation(0, sentence1.length(), sentence1, "sentence");
	}

	private TextAnnotation getSentence2Annot() {
		String sentence1 = getSentence1Text();
		String sentence2 = getSentence2Text();
		return factory.createAnnotation(sentence1.length() + 1, sentence1.length() + 1 + sentence2.length(), sentence2,
				"sentence");
	}

	private TextAnnotation getAugSent1aAnnot() {
		String augSent1aMetadata = "AUGSENT\t0\t0\t31";
		int augSent1aStart = getDocumentText().length() + 1 + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER.length()
				+ 1 + augSent1aMetadata.length() + 1;
		TextAnnotation annot = factory.createAnnotation(augSent1aStart, augSent1aStart + getAugSent1aText().length(),
				getAugSent1aText(), "sentence");
		return annot;
	}

	private TextAnnotation getAugSent1bAnnot() {
		String augSent1bMetadata = "AUGSENT\t0\t67\t90";
		int augSent1bStart = getAugSent1aAnnot().getAnnotationSpanEnd() + 1 + augSent1bMetadata.length() + 1;
		TextAnnotation annot = factory.createAnnotation(augSent1bStart, augSent1bStart + getAugSent1bText().length(),
				getAugSent1bText(), "sentence");
		return annot;

	}

	private TextAnnotation getAugSent1cAnnot() {
		String augSent1cMetadata = "AUGSENT\t0\t161\t179";
		int augSent1cStart = getAugSent1bAnnot().getAnnotationSpanEnd() + 1 + augSent1cMetadata.length() + 1;
		TextAnnotation annot = factory.createAnnotation(augSent1cStart, augSent1cStart + getAugSent1cText().length(),
				getAugSent1cText(), "sentence");
		return annot;

	}

	private TextAnnotation getAugSent2aAnnot() {
		String augSent2aMetadata = "AUGSENT\t188\t204\t239";
		int augSent2aStart = getAugSent1cAnnot().getAnnotationSpanEnd() + 1 + augSent2aMetadata.length() + 1;
		TextAnnotation annot = factory.createAnnotation(augSent2aStart, augSent2aStart + getAugSent2aText().length(),
				getAugSent2aText(), "sentence");
		return annot;

	}

	private AugmentedSentence getAugSent1a() {
		return new AugmentedSentence("1", getAugSent1aAnnot(), getSentence1Annot().getAnnotationSpanStart(), 0, 31);
	}

	private AugmentedSentence getAugSent1b() {
		return new AugmentedSentence("2", getAugSent1bAnnot(), getSentence1Annot().getAnnotationSpanStart(), 67, 90);
	}

	private AugmentedSentence getAugSent1c() {
		return new AugmentedSentence("3", getAugSent1cAnnot(), getSentence1Annot().getAnnotationSpanStart(), 161, 179);
	}

	private AugmentedSentence getAugSent2a() {
		return new AugmentedSentence("4", getAugSent2aAnnot(), getSentence2Annot().getAnnotationSpanStart(), 204, 239);
	}

	private List<AugmentedSentence> getAugmentedSentences() {
		return Arrays.asList(getAugSent1a(), getAugSent1b(), getAugSent1c(), getAugSent2a());
	}

	/**
	 * validate the sample sentence spans against the document text to make sure
	 * they are correct
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidateSentenceSpans() throws IOException {
		// confirm that the sentence spans are correct
		for (TextAnnotation sentAnnot : Arrays.asList(getSentence1Annot(), getSentence2Annot(), getAugSent1aAnnot(),
				getAugSent1bAnnot(), getAugSent1cAnnot(), getAugSent2aAnnot())) {
			assertEquals(sentAnnot.getCoveredText(), getDocTextWithAugmentedSection()
					.substring(sentAnnot.getAnnotationSpanStart(), sentAnnot.getAnnotationSpanEnd()));
		}
	}

	/**
	 * validate the sample concept annotation spans against the document text to
	 * make sure they are correct
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidateConceptAnnotSpans() throws IOException {
		for (ConceptAnnot ca : ConceptAnnot.values()) {
			TextAnnotation annot = getConceptAnnot(ca);
			assertEquals(annot.getCoveredText(), getDocTextWithAugmentedSection()
					.substring(annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd()));
		}
	}

	@Test
	public void testPropagateHybridAbbreviations() throws IOException {

		TextDocument abbrevDoc = getAbbreviationDoc();

		Set<TextAnnotation> updatedAnnots = ConceptPostProcessingFn.propagateHybridAbbreviations(getOgerConceptAnnots(),
				abbrevDoc.getAnnotations(), "PMID:12345", getDocTextWithAugmentedSection());

		// the ES cell and ES annotations should have been added
		Set<TextAnnotation> expectedUpdatedAnnots = new HashSet<TextAnnotation>(getOgerConceptAnnots());
		expectedUpdatedAnnots.add(getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT));
		expectedUpdatedAnnots.add(getConceptAnnot(ConceptAnnot.ORIG_ES_CELLS_ANNOT));

		assertEquals(expectedUpdatedAnnots, updatedAnnots);

		// test that the hybrid abbreviation annotation is removed from the abbrev set
		// -- Actually, we shouldn't remove these abbreviations as they could appear on
		// their own without the hybrid extra text.
		Set<TextAnnotation> longFormAnnots = ConceptPostProcessingFn.getLongFormAnnots(abbrevDoc.getAnnotations());
		Set<TextAnnotation> expectedLongAbbrevAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(getAldh1AbbrevAnnot(), getPdAbbrevAnnot(), getAldh1a3AbbrevAnnot(), getEsAbbrevAnnot()));

		assertEquals(expectedLongAbbrevAnnots, longFormAnnots);

	}

	/**
	 * Test that we get a mapping from the AugmentedSentence to the concepts that
	 * overlap with the augmented sentence. The concepts, however, should have had
	 * their spans transformed such that they are relative to the original sentence.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapSentToAugConceptAnnots() throws IOException {
		Map<AugmentedSentence, Set<TextAnnotation>> mapOrigSentToConceptAnnots = ConceptPostProcessingFn
				.mapSentToConceptAnnots(getAugmentedSentences(), getOgerConceptAnnots(), Overlap.AUG_SENTENCE);

		Map<AugmentedSentence, Set<TextAnnotation>> expectedMap = new HashMap<AugmentedSentence, Set<TextAnnotation>>();

		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent1()) {
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1a(), conceptAnnot, expectedMap);
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1b(), conceptAnnot, expectedMap);
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(), conceptAnnot, expectedMap);
		}
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(),
				getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT), expectedMap);

		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent2()) {
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent2a(), conceptAnnot, expectedMap);
		}

		assertEquals(expectedMap.size(), mapOrigSentToConceptAnnots.size());

		assertEquals(expectedMap.get(getAugSent1a()), mapOrigSentToConceptAnnots.get(getAugSent1a()));
		assertEquals(expectedMap.get(getAugSent1b()), mapOrigSentToConceptAnnots.get(getAugSent1b()));
		assertEquals(expectedMap.get(getAugSent1c()), mapOrigSentToConceptAnnots.get(getAugSent1c()));
		assertEquals(expectedMap.get(getAugSent2a()), mapOrigSentToConceptAnnots.get(getAugSent2a()));

	}

	/**
	 * same as above, but the concept annotations matched should now overlap with
	 * the original sentence (not a sentence in the augmented section)
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapSentToOrigConceptAnnots() throws IOException {
		Map<AugmentedSentence, Set<TextAnnotation>> mapOrigSentToConceptAnnots = ConceptPostProcessingFn
				.mapSentToConceptAnnots(getAugmentedSentences(), getOgerConceptAnnots(), Overlap.ORIG_SENTENCE);

		Map<AugmentedSentence, Set<TextAnnotation>> expectedMap = new HashMap<AugmentedSentence, Set<TextAnnotation>>();

		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent1()) {
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1a(), conceptAnnot, expectedMap);
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1b(), conceptAnnot, expectedMap);
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(), conceptAnnot, expectedMap);
		}
		for (TextAnnotation conceptAnnot : getOgerConceptAnnotsSent2()) {
			CollectionsUtil.addToOne2ManyUniqueMap(getAugSent2a(), conceptAnnot, expectedMap);
		}

		assertEquals(expectedMap.size(), mapOrigSentToConceptAnnots.size());

		assertEquals(expectedMap.get(getAugSent1a()), mapOrigSentToConceptAnnots.get(getAugSent1a()));
		assertEquals(expectedMap.get(getAugSent1b()), mapOrigSentToConceptAnnots.get(getAugSent1b()));
		assertEquals(expectedMap.get(getAugSent1c()), mapOrigSentToConceptAnnots.get(getAugSent1c()));
		assertEquals(expectedMap.get(getAugSent2a()), mapOrigSentToConceptAnnots.get(getAugSent2a()));

	}

	@Test
	public void testMapAugSentToAugConceptAnnots() throws IOException {
		Map<AugmentedSentence, Set<TextAnnotation>> mapAugSentToConceptAnnots = ConceptPostProcessingFn
				.mapAugSentToAugConceptAnnots(getAugmentedSentences(), getOgerConceptAnnots());

		Map<AugmentedSentence, Set<TextAnnotation>> expectedMap = new HashMap<AugmentedSentence, Set<TextAnnotation>>();

		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1a(), getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1a(), getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1a(), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1b(), getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1b(), getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1b(), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(), getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(), getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent1c(),
				getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT), expectedMap);

		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent2a(), getConceptAnnot(ConceptAnnot.ALDH1A3_LONG_ANNOT),
				expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(getAugSent2a(), getConceptAnnot(ConceptAnnot.ALDH_LONG_ANNOT),
				expectedMap);

		assertEquals(expectedMap.size(), mapAugSentToConceptAnnots.size());
		assertEquals(expectedMap, mapAugSentToConceptAnnots);
	}

	@Test
	public void testMapConceptsToAbbrevAnnots() throws IOException {
		Map<TextAnnotation, TextAnnotation> mapConceptsToAbbrevAnnots = ConceptPostProcessingFn
				.mapConceptsToAbbrevAnnots(getOgerConceptAnnots(), getAbbreviationDoc().getAnnotations());

		Map<TextAnnotation, TextAnnotation> expectedMap = new HashMap<TextAnnotation, TextAnnotation>();

		expectedMap.put(getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT), getAldh1AbbrevAnnot());
		expectedMap.put(getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT), getPdAbbrevAnnot());
		expectedMap.put(getConceptAnnot(ConceptAnnot.ALDH1A3_LONG_ANNOT), getAldh1a3AbbrevAnnot());
		expectedMap.put(getConceptAnnot(ConceptAnnot.ALDH_LONG_ANNOT), getAldh1a3AbbrevAnnot());
		expectedMap.put(getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT), getEsAbbrevAnnot());

		assertEquals(expectedMap, mapConceptsToAbbrevAnnots);

	}

	/**
	 * test that the {@link AugmentedSentence} objects are properly parsed from the
	 * augmented section of the doc text
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetAugmentedSentences() throws IOException {
		List<AugmentedSentence> augmentedSentences = ConceptPostProcessingFn
				.getAugmentedSentences(getDocTextWithAugmentedSection(), docId);

		assertEquals(getAugmentedSentences(), augmentedSentences);
	}

	@Test
	public void testFindConceptsOnlyInAugmentedText() throws IOException {

		Set<TextAnnotation> conceptsOnlyInAugmentedText = ConceptPostProcessingFn
				.findConceptsOnlyInAugmentedText(getOgerConceptAnnots(), getDocTextWithAugmentedSection(), docId);

		Set<TextAnnotation> expectedConcepts = new HashSet<TextAnnotation>(
				Arrays.asList(getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT)));

		assertEquals(expectedConcepts, conceptsOnlyInAugmentedText);

	}

	@Test
	public void testMapShortFormPlusTextToConceptId() throws IOException {
		Map<TextAnnotation, TextAnnotation> conceptToAbbrevMap = new HashMap<TextAnnotation, TextAnnotation>();

		conceptToAbbrevMap.put(getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT), getEsAbbrevAnnot());

		Map<String, String> shortFormPlusTextToConceptIdMap = ConceptPostProcessingFn
				.mapShortFormPlusTextToConceptId(conceptToAbbrevMap, getDocumentText());

		Map<String, String> expectedMap = new HashMap<String, String>();
		expectedMap.put("ES cells", "CL:1");

		assertEquals(expectedMap, shortFormPlusTextToConceptIdMap);

	}

	@Test
	public void testfilterNewConceptAnnot() throws IOException {

		TextAnnotation bestConceptAnnot = getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		TextAnnotation tooShortConceptAnnot = factory.createAnnotation(bestConceptAnnot.getAnnotationSpanStart(),
				bestConceptAnnot.getAnnotationSpanStart() + 4, "embr", "CL:1");

		Set<TextAnnotation> augConceptAnnots = new HashSet<TextAnnotation>();
		// this is the one that will be returned b/c it encompasses the entire
		// abbreviation
		augConceptAnnots.add(tooShortConceptAnnot);

		TextAnnotation outputConceptAnnot = ConceptPostProcessingFn.filterNewConceptAnnot(augConceptAnnots,
				getAugSent1c());

		assertNull("should be null b/c it does not encompass the entire abbreviation", outputConceptAnnot);

		TextAnnotation shorterConceptAnnot = factory.createAnnotation(bestConceptAnnot.getAnnotationSpanStart(),
				bestConceptAnnot.getAnnotationSpanEnd() - 1, "embr", "CL:1");

		augConceptAnnots = new HashSet<TextAnnotation>();
		augConceptAnnots.add(bestConceptAnnot);
		augConceptAnnots.add(tooShortConceptAnnot);
		augConceptAnnots.add(shorterConceptAnnot);

		outputConceptAnnot = ConceptPostProcessingFn.filterNewConceptAnnot(augConceptAnnots, getAugSent1c());
		assertNotNull(outputConceptAnnot);
		assertEquals(bestConceptAnnot, outputConceptAnnot);

	}

	/**
	 * trying to figure out why Enhanced S-cone syndrome appears to be excluded
	 * during post-processing
	 * 
	 * <pre>
	 * 		12345	cell	9	15	S-cone	S cone cell	CL:0003050		S1	CL
		12345	molecular_function	57	71	photoreceptors	photoreceptor activity	GO:0009881		S1	GO_MF
		12345	biological_process	233	248	dark adaptation	dark adaptation	GO:1990603		S1	GO_BP
		12345	biological_process	267	278	sensitivity	sensitization	GO:0046960		S1	GO_BP
		12345	disease	0	24	Enhanced S-cone syndrome	enhanced S-cone syndrome	MONDO:0100288		S1	MONDO
		12345	disease	86	101	night blindness	night blindness	MONDO:0004588		S1	MONDO
		12345	phenotype	86	101	night blindness	Nyctalopia	HP:0000662		S1	HP
		12345	phenotype	138	164	abnormal electroretinogram	Abnormal electroretinogram	HP:0000512		S1	HP
		12345	procedure	147	164	electroretinogram	ERG - electroretinography	SNOMEDCT:6615001		S1	SNOMEDCT
		12345	protein	166	169	ERG	PR:P81270	PR:000007173		S1	PR
		12345	procedure	267	278	sensitivity	Sensitivity	SNOMEDCT:14788002		S1	SNOMEDCT
		12345	protein	286	289	ERG	PR:P81270	PR:000007173		S1	PR
		12345	protein	26	30	ESCS	PR:Q9Y5X4	PR:000011403		S1	PR
		12345	protein	166	169	ERG	PR:000007173|PR:P11308	PR:000007173		S1	PR
		12345	protein	286	289	ERG	PR:000007173|PR:P11308	PR:000007173		S1	PR
	 * 
	 * </pre>
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testPostProcess() throws FileNotFoundException, IOException {

		String docId = "craft-16110338";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		Map<String, String> idToOgerDictEntriesMapPart2 = new HashMap<String, String>();
		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();

		TextAnnotation abbrevAnnot1 = factory.createAnnotation(0, 24, "Enhanced S-cone syndrome", "long_form");
		TextAnnotation abbrevAnnot2 = factory.createAnnotation(26, 30, "ESCS", "short_form");
		DefaultComplexSlotMention csm = new DefaultComplexSlotMention("has_short_form");
		csm.addClassMention(abbrevAnnot2.getClassMention());
		abbrevAnnot1.getClassMention().addComplexSlotMention(csm);

		TextAnnotation abbrevAnnot3 = factory.createAnnotation(147, 164, "electroretinogram", "long_form");
		TextAnnotation abbrevAnnot4 = factory.createAnnotation(166, 169, "ERG", "short_form");
		DefaultComplexSlotMention csm2 = new DefaultComplexSlotMention("has_short_form");
		csm2.addClassMention(abbrevAnnot4.getClassMention());
		abbrevAnnot3.getClassMention().addComplexSlotMention(csm2);

		abbrevAnnots.add(abbrevAnnot1);
		abbrevAnnots.add(abbrevAnnot3);

//		CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0100288", "enhanced S-cone syndrome", idToOgerDictEntriesMap);
		idToOgerDictEntriesMap.put("MONDO:0100288", "enhanced S-cone syndrome");

		String entries = "short-wavelength-sensitive (S) cone|S cone|short wavelength sensitive cone|S-cone photoreceptor|S-cone|short- (S) wavelength-sensitive cone|S cone cell|short-wavelength sensitive cone|S-(short-wavelength sensitive) cone";
		idToOgerDictEntriesMap.put("CL:0003050", entries);

//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("CL:0003050", s, idToOgerDictEntriesMap);
//		}

		entries = "Nr2e3|RP37|rd7|UniProtKB:Q9QXZ7-1, 1-352|NR2E3|photoreceptor-specific nuclear receptor|mNR2E3/iso:m2|mNR2E3/iso:m1|PNR|RT06950p1|RNR|nuclear receptor subfamily 2 group E member 3|A930035N01Rik|photoreceptor-specific nuclear receptor isoform m2|photoreceptor-specific nuclear receptor isoform m1|mNR2E3|hNR2E3/iso:Short|Rp37|photoreceptor-specific nuclear receptor short form|photoreceptor-specific nuclear receptor long form|photoreceptor-specific nuclear receptor isoform Long|hormone receptor 51|hNR2E3/iso:Long|fly-NR2E3|photoreceptor-specific nuclear receptor isoform Short|nuclear receptor subfamily 2, group E, member 3|hNR2E3|ESCS|retina-specific nuclear receptor";
		idToOgerDictEntriesMap.put("PR:000011403", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("PR:000011403", s, idToOgerDictEntriesMap);
//		}

		entries = "photoreceptor|photoreceptor activity";
		idToOgerDictEntriesMap.put("GO:0009881", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("GO:0009881", s, idToOgerDictEntriesMap);
//		}

		entries = "nyctalopia|night blindness";
		idToOgerDictEntriesMap.put("MONDO:0004588", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0004588", s, idToOgerDictEntriesMap);
//		}

		entries = "Night blindness|Nyctalopia|Night-blindness|Poor night vision";
		idToOgerDictEntriesMap.put("HP:0000662", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("HP:0000662", s, idToOgerDictEntriesMap);
//		}

		entries = "Abnormal electroretinography|ERG abnormal|Abnormal ERG|Abnormal electroretinogram";
		idToOgerDictEntriesMap.put("HP:0000512", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("HP:0000512", s, idToOgerDictEntriesMap);
//		}

		entries = "Electroretinography|ERG - electroretinography|Electroretinogram|Electroretinography with medical evaluation|Electroretinogram with medical evaluation|ERG - Electroretinography|Electroretinography with medical evaluation (procedure)|Electroretinography (procedure)";
		idToOgerDictEntriesMap.put("SNOMEDCT:6615001", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:6615001", s, idToOgerDictEntriesMap);
//		}

		entries = "mERG/iso:7|erg-3|hERG/iso:h6|transcriptional regulator ERG isoform h5|transcriptional regulator ERG isoform 1|p55|transcriptional regulator ERG isoform h6|transcriptional regulator ERG isoform 2|mERG/iso:exon4-containing|transcriptional regulator ERG isoform 5|transcriptional regulator ERG isoform 6|transcriptional regulator ERG isoform 3|transcriptional regulator ERG isoform 4|transcriptional regulator ERG isoform h4|transcriptional regulator ERG isoform ERG-2|transcriptional regulator ERG isoform 7|transcriptional regulator ERG isoform ERG-3|hERG/iso:ERG-3|hERG/iso:ERG-2|mERG|mERG/iso:exon3-containing|hERG|transforming protein ERG|transcriptional regulator ERG isoform ERG-1|D030036I24Rik|transcriptional regulator ERG|transcriptional regulator Erg|transcriptional regulator ERG isoform containing exon 3|transcriptional regulator ERG isoform containing exon 4|ETS transcription factor|ETS transcription factor ERG|chick-ERG|hERG/iso:h4|hERG/iso:h5|ERG/iso:4|hERG/iso:5|ERG/iso:3|mERG/iso:2|ERG/iso:1|mERG/iso:1|ERG|Erg|mERG/iso:6|mERG/iso:5|mERG/iso:3";
		idToOgerDictEntriesMap.put("PR:000007173", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007173", s, idToOgerDictEntriesMap);
//		}

//		CollectionsUtil.addToOne2ManyUniqueMap("GO:1990603", "dark adaptation", idToOgerDictEntriesMap);
		idToOgerDictEntriesMap.put("GO:1990603", "dark adaptation");

		entries = "Antimicrobial susceptibility test (procedure)|Sensitivity|Antimicrobial susceptibility test|Antimicrobial susceptibility test, NOS|Sensitivities";
		idToOgerDictEntriesMap.put("SNOMEDCT:14788002", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:14788002", s, idToOgerDictEntriesMap);
//		}

//		CollectionsUtil.addToOne2ManyUniqueMap("GO:0046960", "sensitization", idToOgerDictEntriesMap);
		idToOgerDictEntriesMap.put("GO:0046960", "sensitization");

		String documentText = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light."
				+ "\n" + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER + "\n";

		TextAnnotation annot1 = factory.createAnnotation(0, 24, "Enhanced S-cone syndrome", "MONDO:0100288");
		TextAnnotation annot2 = factory.createAnnotation(9, 15, "S-cone", "CL:0003050");
		TextAnnotation annot3 = factory.createAnnotation(26, 30, "ESCS", "PR:000011403");
		TextAnnotation annot4 = factory.createAnnotation(57, 71, "photoreceptors", "GO:0009881");
		TextAnnotation annot5 = factory.createAnnotation(86, 101, "night blindness", "MONDO:0004588");
		TextAnnotation annot6 = factory.createAnnotation(86, 101, "night blindness", "HP:0000662");
		TextAnnotation annot7 = factory.createAnnotation(138, 164, "abnormal electroretinogram", "HP:0000512");
		TextAnnotation annot8 = factory.createAnnotation(147, 164, "electroretinogram", "SNOMEDCT:6615001");
		TextAnnotation annot9 = factory.createAnnotation(166, 169, "ERG", "PR:000007173");
		TextAnnotation annot10 = factory.createAnnotation(166, 169, "ERG", "PR:000007173");
		TextAnnotation annot11 = factory.createAnnotation(233, 248, "dark adaptation", "GO:1990603");
		TextAnnotation annot12 = factory.createAnnotation(267, 278, "sensitivity", "SNOMEDCT:14788002");
		TextAnnotation annot13 = factory.createAnnotation(267, 278, "sensitivity", "GO:0046960");
		TextAnnotation annot14 = factory.createAnnotation(286, 289, "ERG", "PR:000007173");
		TextAnnotation annot15 = factory.createAnnotation(286, 289, "ERG", "PR:000007173");
		annot1.setAnnotationID("annot1");
		annot2.setAnnotationID("annot2");
		annot3.setAnnotationID("annot3");
		annot4.setAnnotationID("annot4");
		annot5.setAnnotationID("annot5");
		annot6.setAnnotationID("annot6");
		annot7.setAnnotationID("annot7");
		annot8.setAnnotationID("annot8");
		annot9.setAnnotationID("annot9");
		annot10.setAnnotationID("annot10");
		annot11.setAnnotationID("annot11");
		annot12.setAnnotationID("annot12");
		annot13.setAnnotationID("annot13");
		annot14.setAnnotationID("annot14");
		annot15.setAnnotationID("annot15");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4,
				annot5, annot6, annot7, annot8, annot9, annot10, annot11, annot12, annot13, annot14, annot15));

		Set<TextAnnotation> postProcessedAnnots = ConceptPostProcessingFn.postProcess(docId, extensionToOboMap,
				ncbitaxonPromotionMap, abbrevAnnots, documentText, inputAnnots);

		TextAnnotation annot16 = factory.createAnnotation(26, 30, "ESCS", "MONDO:0100288");

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1,
				// annot2, - excluded b/c it's nested in annot1
				annot16, annot4, annot5,
				// annot6, - excluded b/c it's an HP annot that has the exact same span as a
				// MONDO
				annot7,
				// annot8, - excluded b/c it's nested in annot7
				// annot9, annot10, - excluded because they are a short form of an abbreviation
				annot11, annot12, annot13 // annot13 no longer excluded b/c removespuriousmatches was moved to a
											// separate
											// pipeline. - excluded b/c sensitivity is not close enough to
											// sensitization,
		// annot14, annot15 - excluded because they are a short form of an abbreviation
		));

//		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

	/**
	 * <pre>
	 * 12345	protein	66	69	EWS	PR:Q01844	PR:000007243		S1	PR
	12345	protein	70	74	Pea3	PR:P28322	PR:000007227		S1	PR
	12345	protein	161	164	EWS	PR:Q01844	PR:000007243		S1	PR
	12345	protein	179	183	Pea3	PR:P28322	PR:000007227		S1	PR
	12345	organism	38	42	mice	Mus <genus>	NCBITaxon:10088		S1	NCBITaxon
	12345	organism	38	42	mice	Mus sp.	NCBITaxon:10095		S1	NCBITaxon
	12345	protein	66	69	EWS	PR:Q61545	PR:000007243		S1	PR
	12345	organism	70	73	Pea	Pisum sativum	NCBITaxon:3888		S1	NCBITaxon
	12345	procedure	90	96	fusion	Fusion	SNOMEDCT:122501008		S1	SNOMEDCT
	12345	phenotype	146	159	Ewing sarcoma	Ewing sarcoma	HP:0012254		S1	HP
	12345	disease	146	159	Ewing sarcoma	Ewing sarcoma	MONDO:0012817		S1	MONDO
	12345	protein	161	164	EWS	PR:Q61545	PR:000007243		S1	PR
	12345	sequence_feature	166	170	gene	gene	SO:0000704		S1	SO
	12345	organism	179	182	Pea	Pisum sativum	NCBITaxon:3888		S1	NCBITaxon
	12345	molecular_function	184	195	DNA binding	DNA binding	GO:0003677		S1	GO_MF
	 * </pre>
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	@Ignore("Ignoring this, hopefully temporarily, while we have disabled ncbitaxon promotion b/c the pipeline is stalling during the ncbi promotion step.")
	@Test
	public void testPostProcess2() throws FileNotFoundException, IOException {

		String docId = "craft-15836427";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		Map<String, String> idToOgerDictEntriesMapPart2 = new HashMap<String, String>();
		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();

		String entries = "NCBITaxon:9989|NCBITaxon:1|NCBITaxon:7742|NCBITaxon:2759|NCBITaxon:1437010|NCBITaxon:6072|NCBITaxon:33208|NCBITaxon:131567|NCBITaxon:9347|NCBITaxon:117571|NCBITaxon:117570|NCBITaxon:10066|NCBITaxon:39107|NCBITaxon:7711|NCBITaxon:7776|NCBITaxon:337687|NCBITaxon:314147|NCBITaxon:1338369|NCBITaxon:40674|NCBITaxon:314146|NCBITaxon:32524|NCBITaxon:89593|NCBITaxon:32525|NCBITaxon:32523|NCBITaxon:33213|NCBITaxon:33511|NCBITaxon:1963758|NCBITaxon:8287|NCBITaxon:33154";
		idToOgerDictEntriesMap.put("", entries);
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10088", s, ncbitaxonPromotionMap);
		}

		entries = "NCBITaxon:9989|NCBITaxon:1|NCBITaxon:7742|NCBITaxon:2759|NCBITaxon:1437010|NCBITaxon:6072|NCBITaxon:941326|NCBITaxon:33208|NCBITaxon:131567|NCBITaxon:9347|NCBITaxon:117571|NCBITaxon:117570|NCBITaxon:10088|NCBITaxon:10066|NCBITaxon:39107|NCBITaxon:7711|NCBITaxon:7776|NCBITaxon:337687|NCBITaxon:314147|NCBITaxon:1338369|NCBITaxon:40674|NCBITaxon:314146|NCBITaxon:32524|NCBITaxon:89593|NCBITaxon:32525|NCBITaxon:32523|NCBITaxon:33213|NCBITaxon:33511|NCBITaxon:1963758|NCBITaxon:8287|NCBITaxon:33154";
		idToOgerDictEntriesMap.put("", entries);
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10095", s, ncbitaxonPromotionMap);
		}

		entries = "NCBITaxon:91835|NCBITaxon:3887|NCBITaxon:35493|NCBITaxon:1|NCBITaxon:33090|NCBITaxon:3803|NCBITaxon:2759|NCBITaxon:71240|NCBITaxon:131567|NCBITaxon:131221|NCBITaxon:2231393|NCBITaxon:78536|NCBITaxon:58024|NCBITaxon:58023|NCBITaxon:3398|NCBITaxon:91827|NCBITaxon:163743|NCBITaxon:2233839|NCBITaxon:2233838|NCBITaxon:3814|NCBITaxon:71275|NCBITaxon:1437201|NCBITaxon:2231382|NCBITaxon:1437183|NCBITaxon:72025|NCBITaxon:3193";
		idToOgerDictEntriesMap.put("", entries);
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:3888", s, ncbitaxonPromotionMap);
		}

		TextAnnotation abbrevAnnot1 = factory.createAnnotation(146, 159, "Ewing sarcoma", "long_form");
		TextAnnotation abbrevAnnot2 = factory.createAnnotation(161, 164, "EWS", "short_form");
		DefaultComplexSlotMention csm = new DefaultComplexSlotMention("has_short_form");
		csm.addClassMention(abbrevAnnot2.getClassMention());
		abbrevAnnot1.getClassMention().addComplexSlotMention(csm);

		abbrevAnnots.add(abbrevAnnot1);

		entries = "mEWSR1|Ewsh|bK984G1.4|hEWSR1/iso:EWS-B|Ewing sarcoma breakpoint region 1 protein|RNA-binding protein EWS isoform EWS|hEWSR1/iso:h3|EWS-FLI1|Ewing sarcoma breakpoint region 1|hEWSR1/iso:h6|hEWSR1/iso:h5|hEWSR1/iso:h4|EWS|Ews|RNA-binding protein EWS|ewing sarcoma breakpoint region 1 protein|Ewsr1|RNA-binding protein EWS isoform EWS-B|RNA-binding protein EWS isoform h6|EWS RNA binding protein 1|EWSR1|hEWSR1|RNA-binding protein EWS isoform h3|RNA-binding protein EWS isoform h4|RNA-binding protein EWS isoform h5|EWS oncogene|hEWSR1/iso:EWS";
		idToOgerDictEntriesMap.put("PR:000007243", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007243", s, idToOgerDictEntriesMap);
//		}

		entries = "ETS translocation variant 4 isoform h3|Pea-3|protein PEA3|z-ETV4|E1A-F|ETS translocation variant 4 isoform h2|ETS translocation variant 4 isoform h1|E1AF|ETS translocation variant 4|mETV4|Peas3|PEA3|Pea3|hETV4/iso:h1|polyomavirus enhancer activator 3 homolog|hETV4/iso:h2|hETV4/iso:h3|ETS translocation variant 4 isoform m1|ets variant 4|PEAS3|ETS translocation variant 4 isoform m2|Etv4|ETV4|hETV4|adenovirus E1A enhancer-binding protein|ETS variant transcription factor 4|polyomavirus enhancer activator 3|mETV4/iso:m1|mETV4/iso:m2";
		idToOgerDictEntriesMap.put("PR:000007227", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007227", s, idToOgerDictEntriesMap);
//		}

		entries = "mouse|Mus|mice|Mus <genus>";
		idToOgerDictEntriesMap.put("NCBITaxon:10088", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10088", s, idToOgerDictEntriesMap);
//		}

		entries = "Mus sp.|mice";
		idToOgerDictEntriesMap.put("NCBITaxon:10095", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10095", s, idToOgerDictEntriesMap);
//		}

		entries = "peas|Pisum sativum|garden pea|pea";
		idToOgerDictEntriesMap.put("NCBITaxon:3888", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:3888", s, idToOgerDictEntriesMap);
//		}

		entries = "Fusion procedure (procedure)|Fusion procedure|Fusion";
		idToOgerDictEntriesMap.put("SNOMEDCT:122501008", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:122501008", s, idToOgerDictEntriesMap);
//		}

		entries = "Ewing sarcoma|Ewing's sarcoma";
		idToOgerDictEntriesMap.put("HP:0012254", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("HP:0012254", s, idToOgerDictEntriesMap);
//		}

		entries = "Ewing's family localized tumor|PNET of Thoracopulmonary region|Ewing sarcoma|Ewing's tumor|Ewing's sarcoma|Ewings sarcoma";
		idToOgerDictEntriesMap.put("MONDO:0012817", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0012817", s, idToOgerDictEntriesMap);
//		}

		entries = "gene|genes|INSDC_feature:gene";
		idToOgerDictEntriesMap.put("SO:0000704", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("SO:0000704", s, idToOgerDictEntriesMap);
//		}

//		CollectionsUtil.addToOne2ManyUniqueMap("GO:0003677", "DNA binding", idToOgerDictEntriesMap);
		idToOgerDictEntriesMap.put("GO:0003677", "DNA binding");

		String documentText = "These findings prompted us to analyze mice in which we integrated EWS-Pea3, a break-point fusion product between the amino-terminal domain of the Ewing sarcoma (EWS) gene and the Pea3 DNA binding domain."
				+ "\n" + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER + "\n";

		TextAnnotation annot1 = factory.createAnnotation(38, 42, "mice", "NCBITaxon:10088");
		TextAnnotation annot2 = factory.createAnnotation(38, 42, "mice", "NCBITaxon:10095");
		TextAnnotation annot3 = factory.createAnnotation(66, 69, "EWS", "PR:000007243");
		TextAnnotation annot4 = factory.createAnnotation(66, 69, "EWS", "PR:000007243");
		TextAnnotation annot5 = factory.createAnnotation(70, 73, "Pea", "NCBITaxon:3888");
		TextAnnotation annot6 = factory.createAnnotation(70, 74, "Pea3", "PR:000007227");
		TextAnnotation annot7 = factory.createAnnotation(90, 96, "fusion", "SNOMEDCT:122501008");
		TextAnnotation annot8 = factory.createAnnotation(146, 159, "Ewing sarcoma", "HP:0012254");
		TextAnnotation annot9 = factory.createAnnotation(146, 159, "Ewing sarcoma", "MONDO:0012817");
		TextAnnotation annot10 = factory.createAnnotation(161, 164, "EWS", "PR:000007243");
		TextAnnotation annot11 = factory.createAnnotation(161, 164, "EWS", "PR:000007243");
		TextAnnotation annot12 = factory.createAnnotation(166, 170, "gene", "SO:0000704");
		TextAnnotation annot13 = factory.createAnnotation(179, 182, "Pea", "NCBITaxon:3888");
		TextAnnotation annot14 = factory.createAnnotation(179, 183, "Pea3", "PR:000007227");
		TextAnnotation annot15 = factory.createAnnotation(184, 195, "DNA binding", "GO:0003677");
		annot1.setAnnotationID("annot1");
		annot2.setAnnotationID("annot2");
		annot3.setAnnotationID("annot3");
		annot4.setAnnotationID("annot4");
		annot5.setAnnotationID("annot5");
		annot6.setAnnotationID("annot6");
		annot7.setAnnotationID("annot7");
		annot8.setAnnotationID("annot8");
		annot9.setAnnotationID("annot9");
		annot10.setAnnotationID("annot10");
		annot11.setAnnotationID("annot11");
		annot12.setAnnotationID("annot12");
		annot13.setAnnotationID("annot13");
		annot14.setAnnotationID("annot14");
		annot15.setAnnotationID("annot15");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4,
				annot5, annot6, annot7, annot8, annot9, annot10, annot11, annot12, annot13, annot14, annot15));

		Set<TextAnnotation> postProcessedAnnots = ConceptPostProcessingFn.postProcess(docId, extensionToOboMap,
				ncbitaxonPromotionMap, abbrevAnnots, documentText, inputAnnots);

		// these two annotations get added due to abbreviation propagation
		TextAnnotation annot16 = factory.createAnnotation(66, 69, "EWS", "MONDO:0012817");
		annot16.setAnnotationID("annot16");
		TextAnnotation annot17 = factory.createAnnotation(161, 164, "EWS", "MONDO:0012817");
		annot17.setAnnotationID("annot17");

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1,
				// annot2, - should get promoted to taxon from annot 1
				// annot3, annot4, - removed b/c it is the same text as a short form
				// abbreviation
				// annot5, - gets excluded b/c it is < 4 characters
				annot6, annot7,
				// annot8, - excluded b/c it's a HP annot overlapping a MONDO annot
				annot9,
				// annot10,annot11 - excluded because they are a short form of an abbreviation
				annot12,
				// annot13, - gets excluded b/c it is < 4 characters
				annot14, annot15, annot16, annot17));

//		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

	@Test
	public void testFilterAnnotsInAugmentedDocSection() throws IOException {
		TextDocument abbrevDoc = getAbbreviationDoc();

		Set<TextAnnotation> conceptAnnots = ConceptPostProcessingFn.propagateHybridAbbreviations(getOgerConceptAnnots(),
				abbrevDoc.getAnnotations(), "PMID:12345", getDocTextWithAugmentedSection());

		Set<TextAnnotation> filteredAnnots = ConceptPostProcessingFn.filterAnnotsInAugmentedDocSection(conceptAnnots,
				getDocTextWithAugmentedSection());

		// the ES cell and ES annotations should have been added
		Set<TextAnnotation> expectedFilteredAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(getConceptAnnot(ConceptAnnot.ALDH1_LONG_ANNOT),
						getConceptAnnot(ConceptAnnot.PD_LONG_ANNOT), getConceptAnnot(ConceptAnnot.ALDH1A3_LONG_ANNOT),
						getConceptAnnot(ConceptAnnot.ALDH_LONG_ANNOT), getConceptAnnot(ConceptAnnot.ES_LONG_ANNOT),
						getConceptAnnot(ConceptAnnot.ORIG_EMBRYONIC_STEM_CELLS_ANNOT),
						getConceptAnnot(ConceptAnnot.ORIG_ES_CELLS_ANNOT)));

		assertEquals(expectedFilteredAnnots, filteredAnnots);
	}

	private String get11597317AugmentedDocText() throws IOException {
		return ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMID11597317.augmented.txt",
				CharacterEncoding.UTF_8);
	}

	private List<TextAnnotation> get11597317ClAbbrevAnnots() throws IOException {
		// @formatter:off
				String abbreviationsBionlp = 
						"T1\tlong_form 1999 2019\tdouble-strand breaks\n" + 
						"T2\tshort_form 2021 2025\tDSBs\n" + 
						"T3\tlong_form 2709 2723\tembryonic stem\n" + 
						"T4\tshort_form 2725 2727\tES\n" + 
						"T5\tlong_form 9980 9997\tsingle-strand DNA\n" + 
						"T6\tshort_form 9999 10004\tssDNA\n" + 
						"T7\tlong_form 10011 10028\tdouble-strand DNA\n" + 
						"T8\tshort_form 10030 10035\tdsDNA\n" + 
						"R1\thas_short_form Arg1:T3 Arg2:T4\n" + 
						"R2\thas_short_form Arg1:T7 Arg2:T8\n" + 
						"R3\thas_short_form Arg1:T1 Arg2:T2\n" + 
						"R4\thas_short_form Arg1:T5 Arg2:T6\n" + 
						"";
				// @formatter:on

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument abbrevDoc = bionlpReader.readDocument("craft-11597317", "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(get11597317AugmentedDocText().getBytes()), CharacterEncoding.UTF_8);
		return abbrevDoc.getAnnotations();
	}

	private List<TextAnnotation> get11597317ClOgerAnnots() throws IOException {
//	  0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123
//		Finally, genetic measurements of recombination frequency have shown that Brca1-/- embryonic
//		sentence starts at 14622
//		14712-14622 + 2627 = 
		// @formatter:off
		String abbreviationsBionlp = 
				"T1\tUBERON:1 2717 2721\tstem\n" +
				"T2\tCL:0002322 14704 14729\tembryonic stem      cells\n" 
			;
		// @formatter:on

		System.out.println(String.format("STEM STR: %s", get11597317AugmentedDocText().substring(2719, 2723)));

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument td = bionlpReader.readDocument("craft-11597317", "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(get11597317AugmentedDocText().getBytes()), CharacterEncoding.UTF_8);
		return td.getAnnotations();
	}

	@Test
	public void testCL11597317() throws IOException {
		String docId = "craft-11597317";
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		Map<String, String> idToOgerDictEntriesMapPart2 = new HashMap<String, String>();

		String entries = "ESC|embryonic stem cell";
		idToOgerDictEntriesMap.put("CL:0002322", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("CL:0002322", s, idToOgerDictEntriesMap);
//		}
		entries = "stem";
		idToOgerDictEntriesMap.put("UBERON:1", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("UBERON:1", s, idToOgerDictEntriesMap);
//		}

		Collection<TextAnnotation> abbrevAnnots = get11597317ClAbbrevAnnots();
		Set<TextAnnotation> ogerAnnots = new HashSet<TextAnnotation>(get11597317ClOgerAnnots());
		String augmentedDocText = get11597317AugmentedDocText();

		assertEquals(2, ogerAnnots.size());

		Set<TextAnnotation> postProcessedAnnots = ConceptPostProcessingFn.postProcess(docId, extensionToOboMap,
				ncbitaxonPromotionMap, abbrevAnnots, augmentedDocText, ogerAnnots);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		String coveredText = "embryonic stem (ES) cells";
		int start = augmentedDocText.indexOf(coveredText);
		TextAnnotation esFullAnnot = factory.createAnnotation(start, start + coveredText.length(), coveredText,
				"CL:0002322");

		coveredText = "ES cell";
		start = augmentedDocText.indexOf(coveredText);
		TextAnnotation esCellAnnot = factory.createAnnotation(start, start + coveredText.length(), coveredText,
				"CL:0002322");

		coveredText = "ES cells";
		start = augmentedDocText.indexOf(coveredText);
		TextAnnotation esCellsAnnot = factory.createAnnotation(start, start + coveredText.length(), coveredText,
				"CL:0002322");

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(esFullAnnot, esCellAnnot, esCellsAnnot));

		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

	@Test
	public void testHandlingOfExactOverlapOfConcepts() throws IOException {
		String docId = "craft-11597317";
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		Map<String, String> idToOgerDictEntriesMapPart2 = new HashMap<String, String>();

		String entries = "embryonic stem cell";
		idToOgerDictEntriesMap.put("CL:0002322", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("CL:0002322", s, idToOgerDictEntriesMap);
//		}
		entries = "embryonic stem cell";
		idToOgerDictEntriesMap.put("UBERON:1", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("UBERON:1", s, idToOgerDictEntriesMap);
//		}

		String abbreviationsBionlp = "T1\tUBERON:1 2709 2734\tembryonic stem      cells\n"
				+ "T2\tCL:0002322 2709 2734\tembryonic stem      cells\n";
		System.out.println(String.format("STEM STR: %s", get11597317AugmentedDocText().substring(2709, 2734)));

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument td = bionlpReader.readDocument("craft-11597317", "example",
				new ByteArrayInputStream(abbreviationsBionlp.getBytes()),
				new ByteArrayInputStream(get11597317AugmentedDocText().getBytes()), CharacterEncoding.UTF_8);

		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();
		Set<TextAnnotation> ogerAnnots = new HashSet<TextAnnotation>(td.getAnnotations());
		String augmentedDocText = get11597317AugmentedDocText();

		assertEquals(2, ogerAnnots.size());

		Set<TextAnnotation> postProcessedAnnots = ConceptPostProcessingFn.postProcess(docId, extensionToOboMap,
				ncbitaxonPromotionMap, abbrevAnnots, augmentedDocText, ogerAnnots);

		// the same two annotations should still be present
		assertEquals(2, postProcessedAnnots.size());

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(td.getAnnotations());

		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

	@Test
	public void testHandlingOfPartialOverlapOfConcepts() throws IOException {
		String docId = "craft-11597317";
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();

		String entries = "embryonic stem cell";
		idToOgerDictEntriesMap.put("CL:0002322", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("CL:0002322", s, idToOgerDictEntriesMap);
//		}
		entries = "Brca1-/- embryonic stem";
		idToOgerDictEntriesMap.put("UBERON:1", entries);
//		for (String s : entries.split("\\|")) {
//			CollectionsUtil.addToOne2ManyUniqueMap("UBERON:1", s, idToOgerDictEntriesMap);
//		}

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		TextAnnotation clAnnot = factory.createAnnotation(2709, 2734, "embryonic stem      cells", "CL:0002322");
		TextAnnotation uberonAnnot = factory.createAnnotation(2700, 2721, "Brca1-/- embryonic stem", "UBERON:1");

		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();
		Set<TextAnnotation> ogerAnnots = new HashSet<TextAnnotation>(Arrays.asList(clAnnot, uberonAnnot));
		String augmentedDocText = get11597317AugmentedDocText();

		assertEquals(2, ogerAnnots.size());

		Set<TextAnnotation> postProcessedAnnots = ConceptPostProcessingFn.postProcess(docId, extensionToOboMap,
				ncbitaxonPromotionMap, abbrevAnnots, augmentedDocText, ogerAnnots);

		// the UBERON concept should remain b/c it appears first in the document (this
		// is an arbitrary inclusion criteria that is being used)
		assertEquals(1, postProcessedAnnots.size());

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(Arrays.asList(uberonAnnot));

		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

	@Test
	public void testRemoveAnythingWithASingleBracket() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		TextAnnotation clAnnot = factory.createAnnotation(2709, 2734, "embryonic stem      cells", "CL:0002322");
		TextAnnotation uberonAnnot = factory.createAnnotation(2700, 2721, "Brca1-/- embryonic stem", "UBERON:1");
		TextAnnotation prAnnot1 = factory.createAnnotation(13789, 13793, "PP{V", "PR:000013158");
		TextAnnotation prAnnot2 = factory.createAnnotation(3789, 3793, "PP}V", "PR:000013158");
		TextAnnotation prAnnot3 = factory.createAnnotation(789, 793, "PP]V", "PR:000013158");
		TextAnnotation prAnnot4 = factory.createAnnotation(89, 93, "PP[V", "PR:000013158");
		TextAnnotation prAnnot5 = factory.createAnnotation(113789, 113793, "PP(V", "PR:000013158");
		TextAnnotation prAnnot6 = factory.createAnnotation(213789, 213793, "PP)V", "PR:000013158");
		TextAnnotation prAnnot7 = factory.createAnnotation(313789, 313793, "PP{V}", "PR:000013158");
		TextAnnotation prAnnot8 = factory.createAnnotation(413789, 413793, "PP(V)", "PR:000013158");

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn
				.removeAnythingWithOddBracketCount(new HashSet<TextAnnotation>(Arrays.asList(clAnnot, uberonAnnot,
						prAnnot1, prAnnot2, prAnnot3, prAnnot4, prAnnot5, prAnnot6, prAnnot7, prAnnot8)));

		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(clAnnot, uberonAnnot, prAnnot7, prAnnot8));

		assertEquals(expectedOutputAnnots.size(), outputAnnots.size());
		assertEquals(expectedOutputAnnots, outputAnnots);
	}

}
