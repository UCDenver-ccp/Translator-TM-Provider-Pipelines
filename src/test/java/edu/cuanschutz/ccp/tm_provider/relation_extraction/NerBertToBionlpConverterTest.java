package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.relation_extraction.NerBertToBionlpConverter.EntityDocumentIterator;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class NerBertToBionlpConverterTest {

	@Test
	public void testFixLabel() {
		String tag = "B-CHEBI";
		String lastTag = "O";
		String fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		String expectedLabel = "B-CHEBI";
		assertEquals(expectedLabel, fixedLabel);

		tag = "B-CHEBI";
		lastTag = "B-CL";
		fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		expectedLabel = "B-CHEBI";
		assertEquals(expectedLabel, fixedLabel);

		tag = "O";
		lastTag = "B-CL";
		fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		expectedLabel = "O";
		assertEquals(expectedLabel, fixedLabel);

		tag = "I-CHEBI";
		lastTag = "B-CL";
		fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		expectedLabel = "B-CHEBI";
		assertEquals(expectedLabel, fixedLabel);

		tag = "B-CHEBI";
		lastTag = "B-CHEBI";
		fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		expectedLabel = "I-CHEBI";
		assertEquals(expectedLabel, fixedLabel);

		tag = "B-CHEBI";
		lastTag = "I-CHEBI";
		fixedLabel = EntityDocumentIterator.fixTag(tag, lastTag);
		expectedLabel = "I-CHEBI";
		assertEquals(expectedLabel, fixedLabel);

	}

	private static final String SAMPLE_BERT_INPUT_FILE_NAME = "sample-bert-ner-input.small.txt";
	private static final String SAMPLE_BERT_OUTPUT_FILE_NAME = "sample-bert-ner-output.small.txt";

	@Test
	public void testEntityDocumentIterator() throws IOException {

		InputStream bertInputFileStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(),
				SAMPLE_BERT_INPUT_FILE_NAME);
		InputStream bertOutputFileStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(),
				SAMPLE_BERT_OUTPUT_FILE_NAME);

		EntityDocumentIterator docIter = new EntityDocumentIterator(bertInputFileStream, bertOutputFileStream);

		assertTrue(docIter.hasNext());

		TextDocument doc1 = docIter.next();
		assertEquals("15328538", doc1.getSourceid());
		String doc1Text = doc1.getText();
		assertTrue(doc1Text.startsWith("Loss of Skeletal Muscle"));

		for (TextAnnotation annot : doc1.getAnnotations()) {
			System.out.println(annot);
		}

		assertEquals(2, doc1.getAnnotations().size());

		assertTrue(docIter.hasNext());

		TextDocument doc2 = docIter.next();
		assertEquals("16410827", doc2.getSourceid());
		String doc2Text = doc2.getText();
		assertTrue(doc2Text.startsWith("The Notch Ligand JAG1"));
		assertEquals(10, doc2.getAnnotations().size());

		for (TextAnnotation annot : doc2.getAnnotations()) {
			System.out.println(annot);
		}
		// 155..160 161..164
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("16410827");
		TextAnnotation innerEarAnnot = factory.createAnnotation(155, 164, "inner ear", "UBERON");
		assertTrue(doc2.getAnnotations().contains(innerEarAnnot));

		assertFalse(docIter.hasNext());
	}

}
