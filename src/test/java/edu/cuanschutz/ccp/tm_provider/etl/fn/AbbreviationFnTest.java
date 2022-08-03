package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class AbbreviationFnTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

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

	// This data is from PMC9006252
	@Test
	public void testNullPointerExceptionInRealExample() throws IOException {
		File dir = folder.newFolder();
		File ab3pOutputFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "abbrev.sample.ab3p.results", dir);

		File sentenceAnnotsInBioNLP = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "abbrev.sample.sentences.bionlp", dir);
		File documentTextFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "abbrev.sample.txt", dir);

		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		TextDocument td = reader.readDocument("123456", "source", sentenceAnnotsInBioNLP, documentTextFile,
				CharacterEncoding.UTF_8);
		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		for (TextAnnotation annot : td.getAnnotations()) {
			sentenceToSpanMap.put(annot.getCoveredText(), annot.getAggregateSpan());
		}

		List<String> results = FileReaderUtil.loadLinesFromFile(ab3pOutputFile, CharacterEncoding.UTF_8);
		String documentText = IOUtils.toString(new FileInputStream(documentTextFile),
				CharacterEncoding.UTF_8.getCharacterSetName());

		String serializedAbbreviations = AbbreviationFn.serializeAbbreviations(results, sentenceToSpanMap,
				documentText);
	}

}
