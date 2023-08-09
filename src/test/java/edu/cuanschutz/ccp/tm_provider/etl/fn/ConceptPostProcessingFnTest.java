package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.knowtator.ComplexSlotMention;
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
	 */
	@Test
	public void testPostProcess() {

		String docId = "craft-16110338";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> idToOgerDictEntriesMap = new HashMap<String, Set<String>>();
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
		abbrevAnnot4.getClassMention().addComplexSlotMention(csm2);

		abbrevAnnots.add(abbrevAnnot1);
		abbrevAnnots.add(abbrevAnnot3);

		CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0100288", "enhanced S-cone syndrome", idToOgerDictEntriesMap);

		String entries = "short-wavelength-sensitive (S) cone|S cone|short wavelength sensitive cone|S-cone photoreceptor|S-cone|short- (S) wavelength-sensitive cone|S cone cell|short-wavelength sensitive cone|S-(short-wavelength sensitive) cone";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("CL:0003050", s, idToOgerDictEntriesMap);
		}

		entries = "Nr2e3|RP37|rd7|UniProtKB:Q9QXZ7-1, 1-352|NR2E3|photoreceptor-specific nuclear receptor|mNR2E3/iso:m2|mNR2E3/iso:m1|PNR|RT06950p1|RNR|nuclear receptor subfamily 2 group E member 3|A930035N01Rik|photoreceptor-specific nuclear receptor isoform m2|photoreceptor-specific nuclear receptor isoform m1|mNR2E3|hNR2E3/iso:Short|Rp37|photoreceptor-specific nuclear receptor short form|photoreceptor-specific nuclear receptor long form|photoreceptor-specific nuclear receptor isoform Long|hormone receptor 51|hNR2E3/iso:Long|fly-NR2E3|photoreceptor-specific nuclear receptor isoform Short|nuclear receptor subfamily 2, group E, member 3|hNR2E3|ESCS|retina-specific nuclear receptor";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("PR:000011403", s, idToOgerDictEntriesMap);
		}

		entries = "photoreceptor|photoreceptor activity";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("GO:0009881", s, idToOgerDictEntriesMap);
		}

		entries = "nyctalopia|night blindness";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0004588", s, idToOgerDictEntriesMap);
		}

		entries = "Night blindness|Nyctalopia|Night-blindness|Poor night vision";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("HP:0000662", s, idToOgerDictEntriesMap);
		}

		entries = "Abnormal electroretinography|ERG abnormal|Abnormal ERG|Abnormal electroretinogram";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("HP:0000512", s, idToOgerDictEntriesMap);
		}

		entries = "Electroretinography|ERG - electroretinography|Electroretinogram|Electroretinography with medical evaluation|Electroretinogram with medical evaluation|ERG - Electroretinography|Electroretinography with medical evaluation (procedure)|Electroretinography (procedure)";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:6615001", s, idToOgerDictEntriesMap);
		}

		entries = "mERG/iso:7|erg-3|hERG/iso:h6|transcriptional regulator ERG isoform h5|transcriptional regulator ERG isoform 1|p55|transcriptional regulator ERG isoform h6|transcriptional regulator ERG isoform 2|mERG/iso:exon4-containing|transcriptional regulator ERG isoform 5|transcriptional regulator ERG isoform 6|transcriptional regulator ERG isoform 3|transcriptional regulator ERG isoform 4|transcriptional regulator ERG isoform h4|transcriptional regulator ERG isoform ERG-2|transcriptional regulator ERG isoform 7|transcriptional regulator ERG isoform ERG-3|hERG/iso:ERG-3|hERG/iso:ERG-2|mERG|mERG/iso:exon3-containing|hERG|transforming protein ERG|transcriptional regulator ERG isoform ERG-1|D030036I24Rik|transcriptional regulator ERG|transcriptional regulator Erg|transcriptional regulator ERG isoform containing exon 3|transcriptional regulator ERG isoform containing exon 4|ETS transcription factor|ETS transcription factor ERG|chick-ERG|hERG/iso:h4|hERG/iso:h5|ERG/iso:4|hERG/iso:5|ERG/iso:3|mERG/iso:2|ERG/iso:1|mERG/iso:1|ERG|Erg|mERG/iso:6|mERG/iso:5|mERG/iso:3";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007173", s, idToOgerDictEntriesMap);
		}

		CollectionsUtil.addToOne2ManyUniqueMap("GO:1990603", "dark adaptation", idToOgerDictEntriesMap);

		entries = "Antimicrobial susceptibility test (procedure)|Sensitivity|Antimicrobial susceptibility test|Antimicrobial susceptibility test, NOS|Sensitivities";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:14788002", s, idToOgerDictEntriesMap);
		}

		CollectionsUtil.addToOne2ManyUniqueMap("GO:0046960", "sensitization", idToOgerDictEntriesMap);

		String documentText = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light. ";

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
				idToOgerDictEntriesMap, ncbitaxonPromotionMap, abbrevAnnots, documentText, inputAnnots);

		TextAnnotation annot16 = factory.createAnnotation(26, 30, "ESCS", "MONDO:0100288");

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(annot1, annot2, annot16, annot4, annot5,
						// annot6, - excluded b/c it's an HP annot that has the exact same span as a
						// MONDO
						annot7, annot8,
						// annot9, annot10, - excluded because they are a short form of an abbreviation
						annot11, annot12
				// , annot13 - excluded b/c sensitivity is not close enough to sensitization,
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
	 */
	@Test
	public void testPostProcess2() {

		String docId = "craft-15836427";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> ncbitaxonPromotionMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> idToOgerDictEntriesMap = new HashMap<String, Set<String>>();
		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();

		String entries = "NCBITaxon:9989|NCBITaxon:1|NCBITaxon:7742|NCBITaxon:2759|NCBITaxon:1437010|NCBITaxon:6072|NCBITaxon:33208|NCBITaxon:131567|NCBITaxon:9347|NCBITaxon:117571|NCBITaxon:117570|NCBITaxon:10066|NCBITaxon:39107|NCBITaxon:7711|NCBITaxon:7776|NCBITaxon:337687|NCBITaxon:314147|NCBITaxon:1338369|NCBITaxon:40674|NCBITaxon:314146|NCBITaxon:32524|NCBITaxon:89593|NCBITaxon:32525|NCBITaxon:32523|NCBITaxon:33213|NCBITaxon:33511|NCBITaxon:1963758|NCBITaxon:8287|NCBITaxon:33154";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10088", s, ncbitaxonPromotionMap);
		}

		entries = "NCBITaxon:9989|NCBITaxon:1|NCBITaxon:7742|NCBITaxon:2759|NCBITaxon:1437010|NCBITaxon:6072|NCBITaxon:941326|NCBITaxon:33208|NCBITaxon:131567|NCBITaxon:9347|NCBITaxon:117571|NCBITaxon:117570|NCBITaxon:10088|NCBITaxon:10066|NCBITaxon:39107|NCBITaxon:7711|NCBITaxon:7776|NCBITaxon:337687|NCBITaxon:314147|NCBITaxon:1338369|NCBITaxon:40674|NCBITaxon:314146|NCBITaxon:32524|NCBITaxon:89593|NCBITaxon:32525|NCBITaxon:32523|NCBITaxon:33213|NCBITaxon:33511|NCBITaxon:1963758|NCBITaxon:8287|NCBITaxon:33154";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10095", s, ncbitaxonPromotionMap);
		}

		entries = "NCBITaxon:91835|NCBITaxon:3887|NCBITaxon:35493|NCBITaxon:1|NCBITaxon:33090|NCBITaxon:3803|NCBITaxon:2759|NCBITaxon:71240|NCBITaxon:131567|NCBITaxon:131221|NCBITaxon:2231393|NCBITaxon:78536|NCBITaxon:58024|NCBITaxon:58023|NCBITaxon:3398|NCBITaxon:91827|NCBITaxon:163743|NCBITaxon:2233839|NCBITaxon:2233838|NCBITaxon:3814|NCBITaxon:71275|NCBITaxon:1437201|NCBITaxon:2231382|NCBITaxon:1437183|NCBITaxon:72025|NCBITaxon:3193";
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
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007243", s, idToOgerDictEntriesMap);
		}

		entries = "ETS translocation variant 4 isoform h3|Pea-3|protein PEA3|z-ETV4|E1A-F|ETS translocation variant 4 isoform h2|ETS translocation variant 4 isoform h1|E1AF|ETS translocation variant 4|mETV4|Peas3|PEA3|Pea3|hETV4/iso:h1|polyomavirus enhancer activator 3 homolog|hETV4/iso:h2|hETV4/iso:h3|ETS translocation variant 4 isoform m1|ets variant 4|PEAS3|ETS translocation variant 4 isoform m2|Etv4|ETV4|hETV4|adenovirus E1A enhancer-binding protein|ETS variant transcription factor 4|polyomavirus enhancer activator 3|mETV4/iso:m1|mETV4/iso:m2";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("PR:000007227", s, idToOgerDictEntriesMap);
		}

		entries = "mouse|Mus|mice|Mus <genus>";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10088", s, idToOgerDictEntriesMap);
		}

		entries = "Mus sp.|mice";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:10095", s, idToOgerDictEntriesMap);
		}

		entries = "peas|Pisum sativum|garden pea|pea";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("NCBITaxon:3888", s, idToOgerDictEntriesMap);
		}

		entries = "Fusion procedure (procedure)|Fusion procedure|Fusion";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("SNOMEDCT:122501008", s, idToOgerDictEntriesMap);
		}

		entries = "Ewing sarcoma|Ewing's sarcoma";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("HP:0012254", s, idToOgerDictEntriesMap);
		}

		entries = "Ewing's family localized tumor|PNET of Thoracopulmonary region|Ewing sarcoma|Ewing's tumor|Ewing's sarcoma|Ewings sarcoma";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("MONDO:0012817", s, idToOgerDictEntriesMap);
		}

		entries = "gene|genes|INSDC_feature:gene";
		for (String s : entries.split("\\|")) {
			CollectionsUtil.addToOne2ManyUniqueMap("SO:0000704", s, idToOgerDictEntriesMap);
		}

		CollectionsUtil.addToOne2ManyUniqueMap("GO:0003677", "DNA binding", idToOgerDictEntriesMap);

		String documentText = "These findings prompted us to analyze mice in which we integrated EWS-Pea3, a break-point fusion product between the amino-terminal domain of the Ewing sarcoma (EWS) gene and the Pea3 DNA binding domain.";

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
				idToOgerDictEntriesMap, ncbitaxonPromotionMap, abbrevAnnots, documentText, inputAnnots);

		// these two annotations get added due to abbreviation propagation
		TextAnnotation annot16 = factory.createAnnotation(66, 69, "EWS", "MONDO:0012817");
		annot16.setAnnotationID("annot16");
		TextAnnotation annot17 = factory.createAnnotation(161, 164, "EWS", "MONDO:0012817");
		annot17.setAnnotationID("annot17");

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1,
				// annot2, - should get promoted to taxon from annot 1
				// annot3, annot4, - removed b/c it is the same text as a short form
				// abbreviation
				//annot5, - gets excluded b/c it is < 4 characters
				annot6, annot7,
				// annot8, - excluded b/c it's a HP annot overlapping a MONDO annot
				annot9,
				// annot10,annot11 - excluded because they are a short form of an abbreviation
				annot12, 
				//annot13,  - gets excluded b/c it is < 4 characters
				annot14, annot15, annot16, annot17));

//		assertEquals(expectedPostProcessedAnnots.size(), postProcessedAnnots.size());
		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

}
