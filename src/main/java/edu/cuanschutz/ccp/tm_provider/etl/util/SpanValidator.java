package edu.cuanschutz.ccp.tm_provider.etl.util;

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

		return annotationCoveredText.replaceAll("\\n", " ").equals(coveredTextFromDocument.replaceAll("\\n", " "));
	}

}
