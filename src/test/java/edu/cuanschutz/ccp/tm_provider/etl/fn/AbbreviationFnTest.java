package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.ucdenver.ccp.nlp.core.annotation.Span;

public class AbbreviationFnTest {

	@Test
	public void testSerializeAbbreviations() throws IOException {
		String sentence1 = "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine.";
		String sentence2 = "The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments.";

		String documentText = sentence1 + " " + sentence2;

		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		sentenceToSpanMap.put(sentence1, new Span(0, 134));
		sentenceToSpanMap.put(sentence2, new Span(0 + 135, 132 + 135));

		List<String> results = Arrays.asList(sentence1, "  ALDH1|Aldehyde dehydrogenase 1|0.999613",
				"  PD|Parkinson's disease|0.99818", sentence2, "  ALDH1A3|aldehyde dehydrogenase 1A3|0.999613");

		String serializedAbbreviations = AbbreviationFn.serializeAbbreviations(results, sentenceToSpanMap,
				documentText);

		/*
		 * TODO Note: output order of relations may not be deterministic - so this test
		 * may fail at some point in the future.
		 */
		// @formatter:off
		String expectedAbbreviationsBionlp = 
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

		assertEquals(expectedAbbreviationsBionlp, serializedAbbreviations);
	}

	@Test
	public void testFindNearestShortLongFormSpans() {

		List<Span> shortFormSpans = new ArrayList<Span>(Arrays.asList(new Span(26, 31)));
		List<Span> longFormSpans = new ArrayList<Span>(Arrays.asList(new Span(0, 24)));
		Span[] shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);

		Span[] expectedSpans = new Span[] { new Span(26, 31), new Span(0, 24) };

		assertArrayEquals(expectedSpans, shortLongFormSpans);

		shortFormSpans.add(new Span(55, 60));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(expectedSpans, shortLongFormSpans);

		longFormSpans.add(new Span(45, 50));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(expectedSpans, shortLongFormSpans);

		// add a pair that is closer together and see if it becomes the pair that is
		// returned
		shortFormSpans.add(new Span(77, 79));
		longFormSpans.add(new Span(66, 76));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(new Span[] { new Span(77, 79), new Span(66, 76) }, shortLongFormSpans);

	}

}
