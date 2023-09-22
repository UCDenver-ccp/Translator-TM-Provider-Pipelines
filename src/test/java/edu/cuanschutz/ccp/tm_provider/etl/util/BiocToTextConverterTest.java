package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverter.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.junit.Test;

import com.ctc.wstx.exc.WstxUnexpectedCharException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class BiocToTextConverterTest {

	@Test
	public void testConvert() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "PMC1790863.xml");
		Map<String, TextDocument> docIdToDataMap = convert(new InputStreamReader(sampleDocStream, "UTF-8"));

		String id = "PMC1790863";
		assertNotNull(docIdToDataMap.get(id));
		TextDocument td = docIdToDataMap.get(id);

		assertEquals(132, td.getAnnotations().size());
		assertTrue(td.getText().startsWith("Quantifying Organismal Complexity"));
		assertTrue(td.getText().endsWith("What is a gene?"));

		String expectedText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		assertEquals(expectedText, td.getText());

		for (TextAnnotation ta : td.getAnnotations()) {
			System.out.println(ta.getAggregateSpan() + " -- " + ta.getCoveredText());
			String substring = td.getText().substring(ta.getAggregateSpan().getSpanStart(),
					ta.getAggregateSpan().getSpanEnd());
			if (StringUtil.startsWithRegex(substring, "\\s")) {
				throw new RuntimeException("covered text starts with space |" + substring + "|");
			}
		}

	}

	@Test
	public void testConvert2() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "PMC7500000.xml");
		Map<String, TextDocument> docIdToDataMap = convert(new InputStreamReader(sampleDocStream, "UTF-8"));

		System.out.println("KEYS: " + docIdToDataMap.keySet().toString());

		String id = "PMC7500000";
		assertNotNull(docIdToDataMap.get(id));
		TextDocument td = docIdToDataMap.get(id);

//		assertEquals(132, td.getAnnotations().size());
//		assertTrue(td.getText().startsWith("Quantifying Organismal Complexity"));
//		assertTrue(td.getText().endsWith("What is a gene?"));

		String expectedText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMC7500000.txt",
				CharacterEncoding.UTF_8);
		assertEquals(expectedText, td.getText());

		for (TextAnnotation ta : td.getAnnotations()) {
			System.out.println(ta.getAggregateSpan() + " -- " + ta.getCoveredText());
			String substring = td.getText().substring(ta.getAggregateSpan().getSpanStart(),
					ta.getAggregateSpan().getSpanEnd());
			if (StringUtil.startsWithRegex(substring, "\\s")) {
				throw new RuntimeException("covered text starts with space |" + substring + "|");
			}
		}

		System.out.println(td.getText());
	}

	@Test
	public void testConvert3() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "PMC1069648.xml");
		Map<String, TextDocument> docIdToDataMap = convert(new InputStreamReader(sampleDocStream, "UTF-8"));

		System.out.println("KEYS: " + docIdToDataMap.keySet().toString());

		String id = "PMC1069648";
		assertNotNull(docIdToDataMap.get(id));
		TextDocument td = docIdToDataMap.get(id);

		String expectedText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMC1069648.txt",
				CharacterEncoding.UTF_8);

		System.out.println(td.getText());

		assertEquals(expectedText, td.getText());

		for (TextAnnotation ta : td.getAnnotations()) {
			System.out.println(ta.getAggregateSpan() + " -- " + ta.getCoveredText());
			String substring = td.getText().substring(ta.getAggregateSpan().getSpanStart(),
					ta.getAggregateSpan().getSpanEnd());
			if (StringUtil.startsWithRegex(substring, "\\s")) {
				throw new RuntimeException("covered text starts with space |" + substring + "|");
			}
		}
	}

	@Test(expected = WstxUnexpectedCharException.class)
	public void testConvert_invalidXml() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(),
				"PMC1790863_invalid.xml");
		convert(new InputStreamReader(sampleDocStream, "UTF-8"));

	}

}
