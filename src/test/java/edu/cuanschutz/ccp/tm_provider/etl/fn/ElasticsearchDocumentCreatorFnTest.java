package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.Sentence;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ElasticsearchDocumentCreatorFnTest {

	@Test
	public void testSentenceGetSortedOntologyPrefixes() {
		List<String> conceptIds = Arrays.asList("UBERON_1111111", "HP_2222222", "CL_0000000");
		List<String> sortedOntologyPrefixes = Sentence.getSortedOntologyPrefixes(conceptIds);

		assertEquals(Arrays.asList("_CL", "_HP", "_UBERON"), sortedOntologyPrefixes);
	}

	@Test
	public void testSentenceGetSortedIds() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
		TextAnnotation annot1 = factory.createAnnotation(0, 5, "cell", "CL:0000000");
		TextAnnotation annot2 = factory.createAnnotation(0, 5, "cell", "HP:2222222");
		TextAnnotation annot3 = factory.createAnnotation(0, 5, "cell", "UBERON:1111111");

		Set<TextAnnotation> annots = CollectionsUtil.createSet(annot1, annot2, annot3);
		List<String> sortedIds = Sentence.getSortedIds(annots);

		assertEquals(Arrays.asList("CL:0000000", "HP:2222222", "UBERON:1111111"), sortedIds);
	}

	@Test
	public void testSentenceGetSpanToAnnotMap() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
		TextAnnotation annot1 = factory.createAnnotation(0, 4, "cell", "CL:0000000");
		TextAnnotation annot2 = factory.createAnnotation(134, 139, "cough", "HP:2222222");
		TextAnnotation annot3 = factory.createAnnotation(28, 33, "lungs", "UBERON:1111111");
		TextAnnotation annot4 = factory.createAnnotation(28, 33, "lungs", "FMA:3333333");
		Set<TextAnnotation> annots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4);
		Map<Span, Set<TextAnnotation>> spanToAnnotMap = Sentence.getSpanToAnnotMap(annots);

		Map<Span, Set<TextAnnotation>> expectedMap = new HashMap<Span, Set<TextAnnotation>>();
		expectedMap.put(new Span(0, 4), CollectionsUtil.createSet(annot1));
		expectedMap.put(new Span(134, 139), CollectionsUtil.createSet(annot2));
		expectedMap.put(new Span(28, 33), CollectionsUtil.createSet(annot3, annot4));

		assertEquals(expectedMap, spanToAnnotMap);
	}

	@Test
	public void testSentenceRemoveNestedAnnotations() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
		TextAnnotation annot1 = factory.createAnnotation(0, 4, "cell", "CL:0000000");
		TextAnnotation annot2 = factory.createAnnotation(134, 139, "cough", "HP:2222222");
		TextAnnotation annot3 = factory.createAnnotation(28, 33, "lungs", "UBERON:1111111");
		TextAnnotation annot4 = factory.createAnnotation(20, 33, "lungs", "FMA:3333333");
		Set<TextAnnotation> annots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4);
		Set<TextAnnotation> updatedAnnots = Sentence.removeNestedAnnotations(annots);

		Set<TextAnnotation> expectedAnnots = CollectionsUtil.createSet(annot1, annot2, annot4);

		assertEquals(expectedAnnots, updatedAnnots);
	}

	@Test
	public void testSentenceGetAnnotatedText() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");

		String documentText = "Hydrocodone and oxycodone are prescribed commonly to treat back pain.";
		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 69, documentText, "sentence");

		TextAnnotation annot1 = factory.createAnnotation(0, 11, "Hydrocodone", "CHEBI:5779");
		TextAnnotation annot2 = factory.createAnnotation(0, 11, "Hydrocodone", "DRUGBANK:DB00956");
		TextAnnotation annot3 = factory.createAnnotation(16, 25, "oxycodone", "DRUGBANK:DB00497");
		TextAnnotation annot4 = factory.createAnnotation(16, 25, "oxycodone", "CHEBI:7852");
		TextAnnotation annot5 = factory.createAnnotation(64, 68, "pain", "HP:0012531");
		TextAnnotation annot6 = factory.createAnnotation(59, 68, "back pain", "HP:0003418");
		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6);

		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots, documentText);

		String expectedAnnotatedText = "(Hydrocodone)[CHEBI_5779&DRUGBANK_DB00956&_CHEBI&_DRUGBANK] and (oxycodone)[CHEBI_7852&DRUGBANK_DB00497&_CHEBI&_DRUGBANK] are prescribed commonly to treat (back pain)[HP_0003418&_HP].";

		assertEquals(expectedAnnotatedText, annotatedText);
	}

	@Test
	public void testSentenceGetAnnotatedTextWhenAdjustingForSentenceOffset() {
		int so = 2287;
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");

		String documentText = "Hydrocodone and oxycodone are prescribed commonly to treat back pain.";
		for (int i = 0; i < so; i++) {
			documentText = " " + documentText;
		}
		TextAnnotation sentenceAnnot = factory.createAnnotation(0 + so, 69 + so, documentText, "sentence");

		TextAnnotation annot1 = factory.createAnnotation(0 + so, 11 + so, "Hydrocodone", "CHEBI:5779");
		TextAnnotation annot2 = factory.createAnnotation(0 + so, 11 + so, "Hydrocodone", "DRUGBANK:DB00956");
		TextAnnotation annot3 = factory.createAnnotation(16 + so, 25 + so, "oxycodone", "DRUGBANK:DB00497");
		TextAnnotation annot4 = factory.createAnnotation(16 + so, 25 + so, "oxycodone", "CHEBI:7852");
		TextAnnotation annot5 = factory.createAnnotation(64 + so, 68 + so, "pain", "HP:0012531");
		TextAnnotation annot6 = factory.createAnnotation(59 + so, 68 + so, "back pain", "HP:0003418");
		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6);

		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots, documentText);

		String expectedAnnotatedText = "(Hydrocodone)[CHEBI_5779&DRUGBANK_DB00956&_CHEBI&_DRUGBANK] and (oxycodone)[CHEBI_7852&DRUGBANK_DB00497&_CHEBI&_DRUGBANK] are prescribed commonly to treat (back pain)[HP_0003418&_HP].";

		assertEquals(expectedAnnotatedText, annotatedText);
	}

	@Test
	public void testCreateJsonDocument() throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");

		String documentText = "Hydrocodone and oxycodone are prescribed commonly to treat back pain.";
		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 69, documentText, "sentence");

		TextAnnotation annot1 = factory.createAnnotation(0, 11, "Hydrocodone", "CHEBI:5779");
		TextAnnotation annot2 = factory.createAnnotation(0, 11, "Hydrocodone", "DRUGBANK:DB00956");
		TextAnnotation annot3 = factory.createAnnotation(16, 25, "oxycodone", "DRUGBANK:DB00497");
		TextAnnotation annot4 = factory.createAnnotation(16, 25, "oxycodone", "CHEBI:7852");
		TextAnnotation annot5 = factory.createAnnotation(64, 68, "pain", "HP:0012531");
		TextAnnotation annot6 = factory.createAnnotation(59, 68, "back pain", "HP:0003418");
		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6);

		String jsonDocument = ElasticsearchDocumentCreatorFn.createJsonDocument("a4bd6", sentenceAnnot, conceptAnnots,
				"pmid:12345", documentText);

		String expectedJson = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"sample_elastic_sentence_document.json", CharacterEncoding.UTF_8);

		assertEquals(expectedJson, jsonDocument);

	}

	@Test
	public void testCreateJsonDocumentWithUtf8Char() throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
// gamma = \u03B3
// alpha = \u03B1
// beta = \u03B2

		String alpha = "\u03B1";
		String beta = "\u03B2";
		String gamma = "\u03B3";

		String documentText = String.format(
				"Hydrocodone-%s and oxycodone-%s are prescribed commonly to treat %sback pain.", alpha, beta, gamma);
		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 74, documentText, "sentence");

		TextAnnotation annot1 = factory.createAnnotation(0, 13, String.format("Hydrocodone-%s", alpha), "CHEBI:5779");
		TextAnnotation annot2 = factory.createAnnotation(0, 13, String.format("Hydrocodone-%s", alpha),
				"DRUGBANK:DB00956");
		TextAnnotation annot3 = factory.createAnnotation(18, 29, String.format("oxycodone-%s", beta),
				"DRUGBANK:DB00497");
		TextAnnotation annot4 = factory.createAnnotation(18, 29, String.format("oxycodone-%s", beta), "CHEBI:7852");
		TextAnnotation annot5 = factory.createAnnotation(69, 73, "pain", "HP:0012531");
		TextAnnotation annot6 = factory.createAnnotation(63, 73, String.format("%sback pain", gamma), "HP:0003418");
		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6);

		String jsonDocument = ElasticsearchDocumentCreatorFn.createJsonDocument("a4bd6", sentenceAnnot, conceptAnnots,
				"pmid:12345", documentText);

		String expectedJson = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"sample_elastic_sentence_document_with_utf8.json", CharacterEncoding.UTF_8);

		assertEquals(expectedJson, jsonDocument);

	}

//	T1	sentence 0 140	In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.
//	T2	sentence 142 192	, showed a greater decrease in glutathione levels.
//	T3	sentence 193 380	Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis.
//	T4	sentence 381 558	These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.

//	T5	DRUGBANK:DB00143 173 184	glutathione
//	T6	CL:0000000 269 273	cell
//	T7	GO:0005623 269 273	cell
//	T8	CL:0000682 303 309	M cell
//	T9	GO:0005623 305 309	cell
//	T10	CL:0000000 305 309	cell
//	T11	GO:0022403 305 322	cell cycle phases
//	T12	GO:0005623 346 350	cell
//	T13	CL:0000000 346 350	cell
//	T14	GO:0016209 462 473	antioxidant
//	T15	CHEBI:22586 462 473	antioxidant

//	@Test
//	public void testCreateJsonDocumentPmid31000267Sentence1() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
////		T1	sentence 0 140	In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 140,
//				"In-vitro antioxidant capacity and cytoprotective/cytotoxic effects upon Caco-2 cells of red tilapia (Oreochromis spp.) viscera hydrolysates.",
//				"sentence");
//
////		T1	CHEBI:22586 9 20	antioxidant
////		T2	GO:0016209 9 20	antioxidant
////		T3	GO:0005623 79 84	cells
////		T4	CL:0000000 79 84	cells
//
//		TextAnnotation annot1 = factory.createAnnotation(9, 20, "antioxidant", "CHEBI:22586");
//		TextAnnotation annot2 = factory.createAnnotation(9, 20, "antioxidant", "GO:0016209");
//		TextAnnotation annot3 = factory.createAnnotation(79, 84, "cells", "GO:0005623");
//		TextAnnotation annot4 = factory.createAnnotation(79, 84, "cells", "CL:0000000");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4);
//
//		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}
//
//	@Test
//	public void testCreateJsonDocumentPmid31000267Sentence2() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
////		T2	sentence 142 192	, showed a greater decrease in glutathione levels.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(142, 192,
//				", showed a greater decrease in glutathione levels.", "sentence");
//
////		T5	DRUGBANK:DB00143 173 184	glutathione
//
//		TextAnnotation annot1 = factory.createAnnotation(173, 184, "glutathione", "DRUGBANK:DB00143");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1);
//
//		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

	@Test
	public void testRemoveOverlappingAnnotationsPmid31000267Sentence3() throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");

//		T6	CL:0000000 269 273	cell
//		T7	GO:0005623 269 273	cell
//		T8	CL:0000682 303 309	M cell
//		T9	GO:0005623 305 309	cell
//		T10	CL:0000000 305 309	cell
//		T11	GO:0022403 305 322	cell cycle phases
//		T12	GO:0005623 346 350	cell
//		T13	CL:0000000 346 350	cell

		TextAnnotation annot1 = factory.createAnnotation(269, 273, "cell", "CL:0000000");
		TextAnnotation annot2 = factory.createAnnotation(269, 273, "cell", "GO:0005623");
		TextAnnotation annot3 = factory.createAnnotation(303, 309, "M cell", "CL:0000682");
		TextAnnotation annot4 = factory.createAnnotation(305, 309, "cell", "GO:0005623");
		TextAnnotation annot5 = factory.createAnnotation(305, 309, "cell", "CL:0000000");
		TextAnnotation annot6 = factory.createAnnotation(305, 322, "cell cycle phases", "GO:0022403");
		TextAnnotation annot7 = factory.createAnnotation(346, 350, "cell", "GO:0005623");
		TextAnnotation annot8 = factory.createAnnotation(346, 350, "cell", "CL:0000000");
		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6,
				annot7, annot8);

		Set<TextAnnotation> updatedAnnots = Sentence.removeNestedAnnotations(conceptAnnots);

		Set<TextAnnotation> expectedAnnots = CollectionsUtil.createSet(annot1, annot2, annot6, annot7, annot8);

		assertEquals(expectedAnnots, updatedAnnots);

	}

//	@Test
//	public void testCreateJsonDocumentPmid31000267Sentence3() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
////		T3	sentence 193 380	Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(193, 380,
//				"Moreover, FRTVH-V allowed for a recovery close to that of control levels of cell proportions in the G1 and G2/M cell cycle phases; and a decrease in the cell proportion in late apoptosis.",
//				"sentence");
//
////		T6	CL:0000000 269 273	cell
////		T7	GO:0005623 269 273	cell
////		T8	CL:0000682 303 309	M cell
////		T9	GO:0005623 305 309	cell
////		T10	CL:0000000 305 309	cell
////		T11	GO:0022403 305 322	cell cycle phases
////		T12	GO:0005623 346 350	cell
////		T13	CL:0000000 346 350	cell
//
//		TextAnnotation annot1 = factory.createAnnotation(269, 273, "cell", "CL:0000000");
//		TextAnnotation annot2 = factory.createAnnotation(269, 273, "cell", "GO:0005623");
//		TextAnnotation annot3 = factory.createAnnotation(303, 309, "M cell", "CL:0000682");
//		TextAnnotation annot4 = factory.createAnnotation(305, 309, "cell", "GO:0005623");
//		TextAnnotation annot5 = factory.createAnnotation(305, 309, "cell", "CL:0000000");
//		TextAnnotation annot6 = factory.createAnnotation(305, 322, "cell cycle phases", "GO:0022403");
//		TextAnnotation annot7 = factory.createAnnotation(346, 350, "cell", "GO:0005623");
//		TextAnnotation annot8 = factory.createAnnotation(346, 350, "cell", "CL:0000000");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6,
//				annot7, annot8);
//
//		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31000267Sentence4() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
////		T4	sentence 381 558	These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(381, 558,
//				"These results suggest that RTVH-A and FRTVH-V can be beneficial ingredients with antioxidant properties and can have protective effects against ROS-mediated intestinal injuries.",
//				"sentence");
//
////		T14	GO:0016209 462 473	antioxidant
////		T15	CHEBI:22586 462 473	antioxidant
//
//		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "GO:0016209");
//		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "CHEBI:22586");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
//
//		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	T1	sentence 0 88	Mutational analysis of theFAM175A gene in patients with premature ovarian insufficiency.
//	T2	sentence 90 365	The family with sequence similarity 175 member A gene (FAM175A; also known as ABRAXAS1, CCDC98 and ABRA1), a member of the DNA repair family, contributes to the BRCA1 (BRCA1 DNA repair associated)-dependent DNA damage response and is associated with age at natural menopause.
//	T3	sentence 366 497	However, it remains poorly understood whether sequence variants in FAM175A are causative for premature ovarian insufficiency (POI).
//	T4	sentence 498 611	The aim of this study was to investigate whether mutations in the gene FAM175A were present in patients with POI.
//	T5	sentence 612 772	A total of 400 women with idiopathic POI and 498 control women with regular menstruation (306 age-matched women and 192 women over 40 years old) were recruited.
//	T6	sentence 773 912	After Sanger sequencing of FAM175A, functional experiments were carried out to explore the deleterious effects of the identified variation.
//	T7	sentence 913 1169	DNA damage was subsequently induced by mitomycin C (MMC), and DNA repair capacity and G2-M checkpoint activation were evaluated by examining the phosphorylation level of H2AX (H2A histone family, member X) and the percentage of mitotic cells, respectively.
//	T8	sentence 1170 1321	One rare single-nucleotide polymorphism, rs755187051 in gene FAM175A, c.C727G (p.L243V), was identified in two patients but absent in the 498 controls.
//	T9	sentence 1322 1526	The functional experiments demonstrated that overexpression of variant p.L243V in HeLa cells resulted in a similar sensitivity to MMC-induced damage compared with cells transfected with wild-type FAM175A.
//	T10	sentence 1527 1682	Moreover, after treatment with MMC, there were no differences in DNA repair capacity and G2-M checkpoint activation between the mutant and wild-type genes.
//	T11	sentence 1683 1768	Our results suggest that the p.L243V variant of FAM175A may not be causative for POI.
//	T12	sentence 1769 1830	The contribution of FAM175A to POI needs further exploration.

//	T1	PR:000029464 178 184	CCDC98
//	T2	PR:000029464 189 194	ABRA1
//	T3	CHEBI:16991 213 216	DNA
//	T4	SO:0000352 213 216	DNA
//	T5	PR:000004803 251 256	BRCA1
//	T6	PR:000004803 258 263	BRCA1
//	T7	CHEBI:16991 264 267	DNA
//	T8	SO:0000352 264 267	DNA
//	T9	CHEBI:16991 297 300	DNA
//	T10	SO:0000352 297 300	DNA
//	T11	CHEBI:16991 913 916	DNA
//	T12	SO:0000352 913 916	DNA
//	T13	CHEBI:27504 952 963	mitomycin C
//	T14	CHEBI:16991 975 978	DNA
//	T15	SO:0000352 975 978	DNA
//	T16	PR:000041244 1093 1100	histone
//	T17	CHEBI:15358 1093 1100	histone
//	T18	CHEBI:36976	CHEBI:50319 1186 1196	nucleotide
//	T19	SO:0001236 1186 1196	nucleotide
//	T20	CHEBI:16991 1592 1595	DNA
//	T21	SO:0000352 1592 1595	DNA

//	@Test
//	public void testCreateJsonDocumentWithPercent() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 75,
//				"Hydrocodone and oxycodone are prescribed commonly to treat back pain (55%).", "sentence");
//
//		TextAnnotation annot1 = factory.createAnnotation(0, 11, "Hydrocodone", "CHEBI:5779");
//		TextAnnotation annot2 = factory.createAnnotation(0, 11, "Hydrocodone", "DRUGBANK:DB00956");
//		TextAnnotation annot3 = factory.createAnnotation(16, 25, "oxycodone", "DRUGBANK:DB00497");
//		TextAnnotation annot4 = factory.createAnnotation(16, 25, "oxycodone", "CHEBI:7852");
//		TextAnnotation annot5 = factory.createAnnotation(64, 68, "pain", "HP:0012531");
//		TextAnnotation annot6 = factory.createAnnotation(59, 68, "back pain", "HP:0003418");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2, annot3, annot4, annot5, annot6);
//
//		String jsonDocument = ElasticsearchDocumentCreatorFn.createJsonDocument("a4bd6", sentenceAnnot, conceptAnnots,
//				"pmid:12345");
//
//		System.out.println(jsonDocument);
////		String expectedJson = ClassPathUtil.getContentsFromClasspathResource(getClass(),
////				"sample_elastic_sentence_document.json", CharacterEncoding.UTF_8);
////
////		assertEquals(expectedJson, jsonDocument);
//
//	}

//	T1	sentence 0 88	Radioprotective effects of vitamin A against gamma radiation in mouse bone marrow cells.
//	T2	sentence 90 196	Radioprotectors by neutralizing the effects of free radicals, reduce the destructive effects of radiation.
//	T3	sentence 197 343	In this protocol article, the radioprotectory effect of vitamin A on micronuclei induced by gamma radiation was evaluated using micronucleus test.
//	T4	sentence 344 454	Vitamin A was injected intraperitoneally at 100 and 400 mg/kg two hours before 2 Gray (Gy) of gamma radiation.
//	T5	sentence 455 554	Animals were sacrificed after 24 h, and then specimens of the bone marrow were smeared and stained.
//	T6	sentence 555 617	The number of micronuclei were counted in polychromatic cells.
//	T7	sentence 618 760	Both dosage of vitamin A reduced the micronucleus in bone marrow polychromatic erythrocytes (MnPCE) level, which is statistically significant.
//	T8	sentence 761 913	The appropriate amount of vitamin A for protection in mice is 100 mg/kg, which protect the bone marrow of mice against clastogenic effects of radiation.
//	T9	sentence 914 1114	The results of the study showed that vitamin A, possibly with an antioxidant mechanism, eliminates the effects of free radicals from ionizing radiation on bone marrow cells and reduces genetic damage.
//	T10	sentence 1115 1491	•The data of radioprotective effects of vitamin A showed that administration of 100 mg/kg vitamin A to mice prior to 2 Gy of gamma radiation has reduced the micronucleus levels in PCE cells by a factor of 2.62.•Administration of 100 mg/kg vitamin A, which is much smaller than LD50 of vitamin A (LD50 for intraperitoneal injection = 1510 ± 240 mg/kg) can protect mice.•Vitamin
//	T11	sentence 1492 1701	A reduces the harmful effects of ionizing radiation on DNA, due to the antioxidant activity and the trapping of free radicals produced by radiation, and diminish the genetic damage caused by radiation.•Vitamin
//	T12	sentence 1702 1785	A has no effect on the proliferation and differentiation rate of bone marrow cells.

//	T1	DRUGBANK:DB00162 27 36	vitamin A
//	T2	NCBITaxon:10090 64 69	mouse
//	T3	UBERON:0002371 70 81	bone marrow
//	T4	CL:0002092 70 87	bone marrow cells
//	T5	GO:0005623 82 87	cells
//	T6	CL:0000000 82 87	cells
//	T7	DRUGBANK:DB00162 253 262	vitamin A
//	T8	DRUGBANK:DB00162 344 353	Vitamin A
//	T9	UBERON:0002371 517 528	bone marrow
//	T10	CL:0000000 611 616	cells
//	T11	GO:0005623 611 616	cells
//	T12	DRUGBANK:DB00162 633 642	vitamin A
//	T13	UBERON:0002371 671 682	bone marrow
//	T14	CL:0000232 697 709	erythrocytes
//	T15	DRUGBANK:DB00162 787 796	vitamin A
//	T16	NCBITaxon:10088 815 819	mice
//	T17	UBERON:0002371 852 863	bone marrow
//	T18	NCBITaxon:10088 867 871	mice
//	T19	DRUGBANK:DB00162 951 960	vitamin A
//	T20	CHEBI:22586 979 990	antioxidant
//	T21	GO:0016209 979 990	antioxidant
//	T22	UBERON:0002371 1069 1080	bone marrow
//	T23	CL:0002092 1069 1086	bone marrow cells
//	T24	CL:0000000 1081 1086	cells
//	T25	GO:0005623 1081 1086	cells
//	T26	DRUGBANK:DB00162 1155 1164	vitamin A
//	T27	DRUGBANK:DB00162 1205 1214	vitamin A
//	T28	NCBITaxon:10088 1218 1222	mice
//	T29	GO:0005623 1299 1304	cells
//	T30	CL:0000000 1299 1304	cells
//	T31	DRUGBANK:DB00162 1354 1363	vitamin A
//	T32	DRUGBANK:DB00162 1400 1409	vitamin A
//	T33	NCBITaxon:10088 1478 1482	mice
//	T34	DRUGBANK:DB00162 1484 1493	Vitamin A
//	T35	SO:0000352 1547 1550	DNA
//	T36	CHEBI:22586 1563 1574	antioxidant
//	T37	GO:0016209 1563 1574	antioxidant
//	T38	DRUGBANK:DB00162 1694 1703	Vitamin A
//	T39	UBERON:0002371 1767 1778	bone marrow
//	T40	CL:0002092 1767 1784	bone marrow cells
//	T41	GO:0005623 1779 1784	cells
//	T42	CL:0000000 1779 1784	cells

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence1() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
//
////		T1	sentence 0 88	Radioprotective effects of vitamin A against gamma radiation in mouse bone marrow cells.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(0, 88,
//				"Radioprotective effects of vitamin A against gamma radiation in mouse bone marrow cells.", "sentence");
//
////		T1	DRUGBANK:DB00162 27 36	vitamin A
////		T2	NCBITaxon:10090 64 69	mouse
////		T3	UBERON:0002371 70 81	bone marrow
////		T4	CL:0002092 70 87	bone marrow cells
////		T5	GO:0005623 82 87	cells
////		T6	CL:0000000 82 87	cells
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence2() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T2	sentence 90 196	Radioprotectors by neutralizing the effects of free radicals, reduce the destructive effects of radiation.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(90, 196,
//				"Radioprotectors by neutralizing the effects of free radicals, reduce the destructive effects of radiation.",
//				"sentence");
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence3() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T3	sentence 197 343	In this protocol article, the radioprotectory effect of vitamin A on micronuclei induced by gamma radiation was evaluated using micronucleus test.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(197, 343,
//				"In this protocol article, the radioprotectory effect of vitamin A on micronuclei induced by gamma radiation was evaluated using micronucleus test.",
//				"sentence");
//
////		T7	DRUGBANK:DB00162 253 262	vitamin A
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence4() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T4	sentence 344 454	Vitamin A was injected intraperitoneally at 100 and 400 mg/kg two hours before 2 Gray (Gy) of gamma radiation.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(344, 454,
//				"Vitamin A was injected intraperitoneally at 100 and 400 mg/kg two hours before 2 Gray (Gy) of gamma radiation.",
//				"sentence");
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence5() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T5	sentence 455 554	Animals were sacrificed after 24 h, and then specimens of the bone marrow were smeared and stained.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(455, 554,
//				"Animals were sacrificed after 24 h, and then specimens of the bone marrow were smeared and stained.",
//				"sentence");
//
////		T8	DRUGBANK:DB00162 344 353	Vitamin A
////		T9	UBERON:0002371 517 528	bone marrow
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence6() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T6	sentence 555 617	The number of micronuclei were counted in polychromatic cells.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(555, 617,
//				"The number of micronuclei were counted in polychromatic cells.", "sentence");
//
////		T10	CL:0000000 611 616	cells
////		T11	GO:0005623 611 616	cells
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence7() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T7	sentence 618 760	Both dosage of vitamin A reduced the micronucleus in bone marrow polychromatic erythrocytes (MnPCE) level, which is statistically significant.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(618, 760,
//				"Both dosage of vitamin A reduced the micronucleus in bone marrow polychromatic erythrocytes (MnPCE) level, which is statistically significant.",
//				"sentence");
//
////		T12	DRUGBANK:DB00162 633 642	vitamin A
////		T13	UBERON:0002371 671 682	bone marrow
////		T14	CL:0000232 697 709	erythrocytes
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence8() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T8	sentence 761 913	The appropriate amount of vitamin A for protection in mice is 100 mg/kg, which protect the bone marrow of mice against clastogenic effects of radiation.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(761, 913,
//				"The appropriate amount of vitamin A for protection in mice is 100 mg/kg, which protect the bone marrow of mice against clastogenic effects of radiation.",
//				"sentence");
//
////		T15	DRUGBANK:DB00162 787 796	vitamin A
////		T16	NCBITaxon:10088 815 819	mice
////		T17	UBERON:0002371 852 863	bone marrow
////		T18	NCBITaxon:10088 867 871	mice
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence9() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T9	sentence 914 1114	The results of the study showed that vitamin A, possibly with an antioxidant mechanism, eliminates the effects of free radicals from ionizing radiation on bone marrow cells and reduces genetic damage.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(914, 1114,
//				"The results of the study showed that vitamin A, possibly with an antioxidant mechanism, eliminates the effects of free radicals from ionizing radiation on bone marrow cells and reduces genetic damage.",
//				"sentence");
//
////		T19	DRUGBANK:DB00162 951 960	vitamin A
////		T20	CHEBI:22586 979 990	antioxidant
////		T21	GO:0016209 979 990	antioxidant
////		T22	UBERON:0002371 1069 1080	bone marrow
////		T23	CL:0002092 1069 1086	bone marrow cells
////		T24	CL:0000000 1081 1086	cells
////		T25	GO:0005623 1081 1086	cells
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence10() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T10	sentence 1115 1491	•The data of radioprotective effects of vitamin A showed that administration of 100 mg/kg vitamin A to mice prior to 2 Gy of gamma radiation has reduced the micronucleus levels in PCE cells by a factor of 2.62.•Administration of 100 mg/kg vitamin A, which is much smaller than LD50 of vitamin A (LD50 for intraperitoneal injection = 1510 ± 240 mg/kg) can protect mice.•Vitamin
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(1115, 1491,
//				"•The data of radioprotective effects of vitamin A showed that administration of 100 mg/kg vitamin A to mice prior to 2 Gy of gamma radiation has reduced the micronucleus levels in PCE cells by a factor of 2.62.•Administration of 100 mg/kg vitamin A, which is much smaller than LD50 of vitamin A (LD50 for intraperitoneal injection = 1510 ± 240 mg/kg) can protect mice.•Vitamin",
//				"sentence");
//
////		T26	DRUGBANK:DB00162 1155 1164	vitamin A
////		T27	DRUGBANK:DB00162 1205 1214	vitamin A
////		T28	NCBITaxon:10088 1218 1222	mice
////		T29	GO:0005623 1299 1304	cells
////		T30	CL:0000000 1299 1304	cells
////		T31	DRUGBANK:DB00162 1354 1363	vitamin A
////		T32	DRUGBANK:DB00162 1400 1409	vitamin A
////		T33	NCBITaxon:10088 1478 1482	mice
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence11() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T11	sentence 1492 1701	A reduces the harmful effects of ionizing radiation on DNA, due to the antioxidant activity and the trapping of free radicals produced by radiation, and diminish the genetic damage caused by radiation.•Vitamin
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(1492, 1701,
//				"A reduces the harmful effects of ionizing radiation on DNA, due to the antioxidant activity and the trapping of free radicals produced by radiation, and diminish the genetic damage caused by radiation.•Vitamin",
//				"sentence");
//
////		T34	DRUGBANK:DB00162 1484 1493	Vitamin A
////		T35	SO:0000352 1547 1550	DNA
////		T36	CHEBI:22586 1563 1574	antioxidant
////		T37	GO:0016209 1563 1574	antioxidant
////		T38	DRUGBANK:DB00162 1694 1703	Vitamin A
//
////		TextAnnotation annot1 = factory.createAnnotation(462, 473, "antioxidant", "");
////		TextAnnotation annot2 = factory.createAnnotation(462, 473, "antioxidant", "");
////		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot1, annot2);
////		
////		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

//	@Test
//	public void testCreateJsonDocumentPmid31008064Sentence12() throws IOException {
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
////		T12	sentence 1702 1785	A has no effect on the proliferation and differentiation rate of bone marrow cells.
//
//		TextAnnotation sentenceAnnot = factory.createAnnotation(1702, 1785,
//				"A has no effect on the proliferation and differentiation rate of bone marrow cells.", "sentence");
//
////		T38	DRUGBANK:DB00162 1694 1703	Vitamin A
////		T39	UBERON:0002371 1767 1778	bone marrow
////		T40	CL:0002092 1767 1784	bone marrow cells
////		T41	GO:0005623 1779 1784	cells
////		T42	CL:0000000 1779 1784	cells
//
////		TextAnnotation annot1 = factory.createAnnotation(1694, 1703, "Vitamin A", "DRUGBANK:DB00162");
//		TextAnnotation annot2 = factory.createAnnotation(1767, 1778, "bone marrow", "UBERON:0002371");
//		TextAnnotation annot3 = factory.createAnnotation(1767, 1784, "bone marrow cells", "CL:0002092");
//		TextAnnotation annot4 = factory.createAnnotation(1767, 1784, "cells", "GO:0005623");
//		TextAnnotation annot5 = factory.createAnnotation(1767, 1784, "cells", "CL:0000000");
//		Set<TextAnnotation> conceptAnnots = CollectionsUtil.createSet(annot2, annot3, annot4, annot5);
//
//		String annotatedText = Sentence.getAnnotatedText(sentenceAnnot, conceptAnnots);
//
//	}

}
