package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.nio.charset.StandardCharsets;
import java.util.List;

import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.SpanUtils;

/**
 * This class is used to ensure that that character offsets that define an
 * annotation span are correct. It does so by comparing the annotation covered
 * text with the appropriate substring from the entire document text.
 *
 */
public class SpanValidator {

	public static boolean validate(List<Span> spans, String annotationCoveredText, String documentText) {
		String coveredTextFromDocument = SpanUtils.getCoveredText(spans, documentText);

		return validateForceUtf8(annotationCoveredText, coveredTextFromDocument);
	}

	static boolean validateForceUtf8(String annotationCoveredText, String coveredTextFromDocument) {
		// make sure both strings are encoded as UTF-8
		byte[] bytes1 = coveredTextFromDocument.replaceAll("\\n", " ").getBytes(StandardCharsets.UTF_8);
		byte[] bytes2 = annotationCoveredText.replaceAll("\\n", " ").getBytes(StandardCharsets.UTF_8);

		String utf8EncodedString1 = new String(bytes1, StandardCharsets.UTF_8);
		String utf8EncodedString2 = new String(bytes2, StandardCharsets.UTF_8);
		
		System.out.println("STR1: " + utf8EncodedString1);
		System.out.println("STR2: " + utf8EncodedString2);
		

		return utf8EncodedString1.equals(utf8EncodedString2);
	}

}
