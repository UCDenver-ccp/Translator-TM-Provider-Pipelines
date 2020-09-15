package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

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

}
