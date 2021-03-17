package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;

public class ExtractedSentenceTest {

	private static final String Y_000001 = "Y:000001";
	private static final String X_000001 = "X:000001";
	private static final String documentId = "PMID:12345";

	// 1 2 3 4
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence1 = "This sentence has conceptX1 and conceptX2.";
	// sentence: [0 42] conceptX1 [18 27] conceptX2 [32 41]

	// 4 5 6 7 8 9
	// 0123456789012345678901234567890123456789012345678901234
	private static final String sentence2 = "ConceptX1 is in this sentence, and so is conceptY1.";
	// sentence: [43 94] conceptX1 [43 52] conceptY1 [84 93]

	// 0 1 2 3
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence3 = "There are no concepts in this sentence.";
	// sentence: [95 134]

	// 4 5 6 4
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence4 = "ConceptX1 is in this sentence.";
	// sentence: [135 165] conceptX1 [135 144]

	private static final String documentText = sentence1 + " " + sentence2 + " " + sentence3 + " " + sentence4;

	private static final String PLACEHOLDER_X = "@CONCEPTX$";
	private static final String PLACEHOLDER_Y = "@CONCEPTY$";

	@Test
	public void testGetSentenceWithPlaceholders() {

		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1",
				CollectionsUtil.createList(new Span(43 - 43, 52 - 43)), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(new Span(84 - 43, 93 - 43)), PLACEHOLDER_Y, "sentence", sentence2,
				documentText);

		String sentenceWithPlaceholders = es.getSentenceWithPlaceholders();

		String expectedSentenceWithPlaceholders = String.format("%s is in this sentence, and so is %s.", PLACEHOLDER_X,
				PLACEHOLDER_Y);

		assertEquals(expectedSentenceWithPlaceholders, sentenceWithPlaceholders);

	}

	@Test
	public void testGetSpansSingle() {
		List<Span> spans = Arrays.asList(new Span(0, 5));
		String spanStr = spans.toString();

		List<Span> extractedSpans = ExtractedSentence.getSpans(spanStr);

		assertEquals(spans, extractedSpans);
	}

	@Test
	public void testGetSpansMoreThanOne() {
		List<Span> spans = Arrays.asList(new Span(0, 5), new Span(10, 15));
		String spanStr = spans.toString();

		List<Span> extractedSpans = ExtractedSentence.getSpans(spanStr);

		assertEquals(spans, extractedSpans);
	}

	@Test
	public void testFromTsv() {
		String tsv = "Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor ? (@GENE$-?) and interleukin-6 (IL-6) in supernatant, and suppressed the @CHEMICAL$ expressions of TNF-? and IL-6.	PMID:31816576	TNF	PR:000016488	[[150..153]]	mRNA	CHEBI:33699	[[217..221]]	reduce	252		Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor ? (TNF-?) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-? and IL-6.	Isosteroid alkaloids with different chemical structures from Fritillariae cirrhosae bulbus alleviate LPS-induced inflammatory response in RAW 264.7 cells by MAPK signaling pathway.||||||||Isosteroid alkaloids, natural products from Fritillariae Cirrhosae Bulbus, are well known for its antitussive, expectorant, anti-asthmatic and anti-inflammatory properties. However, the anti-inflammatory effect and its mechanism have not been fully explored. In this study, the anti-inflammatory activitives and the potential mechanisms of five isosteroid alkaloids from F. Cirrhosae Bulbus were investigated in lipopolysaccharide (LPS)-induced RAW264.7 macrophage cells. The pro-inflammatory mediators and cytokines were measured by Griess reagent, ELISA and qRT-PCR. The expression of MAPKs was investigated by western blotting. Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor α (TNF-α) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-α and IL-6. Meanwhile, the five isosteroid alkaloids significantly inhibited the phosphorylated activation of mitogen activated protein kinase (MAPK) signaling pathways, including extracellular signal-regulated kinase (ERK1/2), p38 MAPK and c-Jun N-terminal kinase/stress-activated protein kinase (JNK/SAPK). These results demonstrated that isosteroid alkaloids from F. Cirrhosae Bulbus exert anti-inflammatory effects by down-regulating the level of inflammatory mediators via mediation of MAPK phosphorylation in LPS-induced RAW264.7 macrophages, thus could be candidates for the prevention and treatment of inflammatory diseases.";

		ExtractedSentence es = ExtractedSentence.fromTsv(tsv, false);

		String documentId = "PMID:31816576";
		String entityCoveredText1 = "TNF";
		String entityId1 = "PR:000016488";
		List<Span> entitySpan1 = Arrays.asList(new Span(150, 153));
		String entityPlaceholder1 = "@GENE$";
		String entityCoveredText2 = "mRNA";
		String entityId2 = "CHEBI:33699";
		List<Span> entitySpan2 = Arrays.asList(new Span(217, 221));
		String entityPlaceholder2 = "@CHEMICAL$";
		String keyword = "reduce";

		String sentenceText = "Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor ? (TNF-?) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-? and IL-6.";
		String sentenceContext = "Isosteroid alkaloids with different chemical structures from Fritillariae cirrhosae bulbus alleviate LPS-induced inflammatory response in RAW 264.7 cells by MAPK signaling pathway.||||||||Isosteroid alkaloids, natural products from Fritillariae Cirrhosae Bulbus, are well known for its antitussive, expectorant, anti-asthmatic and anti-inflammatory properties. However, the anti-inflammatory effect and its mechanism have not been fully explored. In this study, the anti-inflammatory activitives and the potential mechanisms of five isosteroid alkaloids from F. Cirrhosae Bulbus were investigated in lipopolysaccharide (LPS)-induced RAW264.7 macrophage cells. The pro-inflammatory mediators and cytokines were measured by Griess reagent, ELISA and qRT-PCR. The expression of MAPKs was investigated by western blotting. Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor α (TNF-α) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-α and IL-6. Meanwhile, the five isosteroid alkaloids significantly inhibited the phosphorylated activation of mitogen activated protein kinase (MAPK) signaling pathways, including extracellular signal-regulated kinase (ERK1/2), p38 MAPK and c-Jun N-terminal kinase/stress-activated protein kinase (JNK/SAPK). These results demonstrated that isosteroid alkaloids from F. Cirrhosae Bulbus exert anti-inflammatory effects by down-regulating the level of inflammatory mediators via mediation of MAPK phosphorylation in LPS-induced RAW264.7 macrophages, thus could be candidates for the prevention and treatment of inflammatory diseases.";

		ExtractedSentence expectedEs = new ExtractedSentence(documentId, entityId1, entityCoveredText1, entitySpan1,
				entityPlaceholder1, entityId2, entityCoveredText2, entitySpan2, entityPlaceholder2, keyword,
				sentenceText, sentenceContext);

		assertEquals(expectedEs.getDocumentId(), es.getDocumentId());
		assertEquals(expectedEs.getEntityCoveredText1(), es.getEntityCoveredText1());
		assertEquals(expectedEs.getEntityCoveredText2(), es.getEntityCoveredText2());
		assertEquals(expectedEs.getEntityId1(), es.getEntityId1());
		assertEquals(expectedEs.getEntityId2(), es.getEntityId2());
		assertEquals(expectedEs.getEntitySpan1(), es.getEntitySpan1());
		assertEquals(expectedEs.getEntitySpan2(), es.getEntitySpan2());
		assertEquals(expectedEs.getEntityPlaceholder1(), es.getEntityPlaceholder1());
		assertEquals(expectedEs.getEntityPlaceholder2(), es.getEntityPlaceholder2());
		assertEquals(expectedEs.getSentenceIdentifier(), es.getSentenceIdentifier());
		assertEquals(expectedEs.getSentenceText(), es.getSentenceText());
		assertEquals(expectedEs.getSentenceContext(), es.getSentenceContext());
	}

}
