package edu.cuanschutz.ccp.tm_provider.etl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import edu.ucdenver.ccp.nlp.core.annotation.Span;

public class SpanValidatorTest {

	@Test
	public void testUtf8Conversion() {

		String s = "2′-deoxyinosine";

		System.out.println("slngth: " + s.length());
		byte[] sBytes = s.getBytes();
		String asciiEncodedString = new String(sBytes, StandardCharsets.US_ASCII);
		System.out.println("ASCII: " + asciiEncodedString);

		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		String utf8EncodedString = new String(bytes, StandardCharsets.UTF_8);
		System.out.println("UTF8: " + utf8EncodedString);

		assertEquals(s, utf8EncodedString);
	}

	@Test
	public void testValidateForceUtf8() {
		String s = "2′-deoxyinosine";
		String documentText = "2′-deoxyinosine";

		assertTrue(SpanValidator.validate(Arrays.asList(new Span(0, 15)), s, documentText));
	}

	@Test
	public void testValidate() {
		String s = "T cell";
		String documentText = "T cell";

		assertTrue(SpanValidator.validate(Arrays.asList(new Span(0, 6)), s, documentText));
	}

}
