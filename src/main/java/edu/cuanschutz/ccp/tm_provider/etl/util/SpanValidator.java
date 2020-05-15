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

		return validateForceAscii(annotationCoveredText, coveredTextFromDocument);
	}

	/**
	 * Convert the strings to their ASCII representations prior to comparing them.
	 * This was needed in order to get the validation to work when run in the cloud.
	 * UTF-8 characters were not matching each other correctly, or for some reason
	 * the character encoding was incorrect.
	 * 
	 * @param annotationCoveredText
	 * @param coveredTextFromDocument
	 * @return
	 */
	static boolean validateForceAscii(String annotationCoveredText, String coveredTextFromDocument) {
		// make sure both strings are encoded as UTF-8
		byte[] bytes1 = coveredTextFromDocument.replaceAll("\\n", " ").getBytes(StandardCharsets.US_ASCII);
		byte[] bytes2 = annotationCoveredText.replaceAll("\\n", " ").getBytes(StandardCharsets.US_ASCII);

		String asciiEncodedString1 = new String(bytes1, StandardCharsets.US_ASCII);
		String asciiEncodedString2 = new String(bytes2, StandardCharsets.US_ASCII);

		return asciiEncodedString1.equals(asciiEncodedString2);
	}

}
