package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverter.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.junit.Test;

import com.ctc.wstx.exc.WstxUnexpectedCharException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;

public class BiocToTextConverterTest {

	@Test
	public void testConvert() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "PMC1790863.xml");
		Map<String, TextDocument> docIdToDataMap = convert(sampleDocStream);

		String id = "PMC1790863";
		assertNotNull(docIdToDataMap.get(id));
		TextDocument td = docIdToDataMap.get(id);

		assertEquals(132, td.getAnnotations().size());
		assertTrue(td.getText().startsWith("Quantifying Organismal Complexity"));
		assertTrue(td.getText().endsWith("What is a gene?"));
		
		String expectedText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMC1790863.txt", CharacterEncoding.UTF_8);
		assertEquals(expectedText, td.getText());
		
	}
	
	@Test(expected = WstxUnexpectedCharException.class)
	public void testConvert_invalidXml() throws FactoryConfigurationError, XMLStreamException, IOException {
		InputStream sampleDocStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "PMC1790863_invalid.xml");
		convert(sampleDocStream);
		
	}

}
