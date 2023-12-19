package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.*;

import org.junit.Test;

public class SentenceDpInputBuilderTest {

	@Test
	public void testGetSentenceWithComments() {

		String tsv = "Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of @CHEMICAL$ (NO), tumor necrosis factor ? (@GENE$-?) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-? and IL-6.	PMID:31816576	TNF	PR:000016488	150|153	nitric oxide	CHEBI:12345	106|118	reduce	252		Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric oxide (NO), tumor necrosis factor ? (TNF-?) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-? and IL-6.	abstract	Journal Article|Review	2021	123				";
		ExtractedSentence es = ExtractedSentence.fromTsv(tsv, false);

		String sentenceWithComments = SentenceDpInputBuilderFn.getSentenceWithComments(es);

		StringBuilder expectedSb = new StringBuilder();
		expectedSb.append(String.format("###C: SENTENCE\t%s\t%s\t123\n", "PMID:31816576", es.getSentenceIdentifier()));
		
		// TNF	PR:000016488	150|153	nitric oxide	CHEBI:12345	106|118
		expectedSb.append("###C: ENTITY\tCHEBI:12345\t106|118\tnitric oxide\n");
		expectedSb.append("###C: ENTITY\tPR:000016488\t150|153\tTNF\n");

		String expectedSentenceWithUnderscoredEntities = "Treatment with the five isosteroid alkaloids in appropriate concentrations could reduce the production of nitric_oxide (NO), tumor necrosis factor ? (TNF-?) and interleukin-6 (IL-6) in supernatant, and suppressed the mRNA expressions of TNF-? and IL-6.";
		expectedSb.append(expectedSentenceWithUnderscoredEntities + "\n\n");
		
		System.out.println(sentenceWithComments);
		assertEquals(expectedSb.toString(), sentenceWithComments);

	}

}
