package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultComplexSlotMention;

public class DocumentTextAugmentationFnTest {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	@Test
	public void testAugmentDocumentText() {

		String docId = "craft-16110338";
		String sentence1 = "This is the first sentence in the document.";
		String sentence2 = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light.";
		String sentence3 = "This is the third sentence in the document.";
		String documentText = String.format("%s\n%s\n%s\n", sentence1, sentence2, sentence3);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, sentence1.length(), sentence1, "sentence");
		TextAnnotation sentenceAnnot2 = factory.createAnnotation(sentence1.length() + 1,
				sentence1.length() + 1 + sentence2.length(), sentence2, "sentence");
		TextAnnotation sentenceAnnot3 = factory.createAnnotation(sentence1.length() + 1 + sentence2.length() + 1,
				sentence1.length() + 1 + sentence2.length() + 1 + sentence3.length(), sentence2, "sentence");

		// confirm that the sentence spans are correct
		assertEquals(sentence1,
				documentText.substring(sentenceAnnot1.getAnnotationSpanStart(), sentenceAnnot1.getAnnotationSpanEnd()));
		assertEquals(sentence2,
				documentText.substring(sentenceAnnot2.getAnnotationSpanStart(), sentenceAnnot2.getAnnotationSpanEnd()));
		assertEquals(sentence3,
				documentText.substring(sentenceAnnot3.getAnnotationSpanStart(), sentenceAnnot3.getAnnotationSpanEnd()));

		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();

		int offset = sentenceAnnot2.getAnnotationSpanStart();
		TextAnnotation abbrevAnnot1 = factory.createAnnotation(0 + offset, 24 + offset, "Enhanced S-cone syndrome",
				"long_form");
		TextAnnotation abbrevAnnot2 = factory.createAnnotation(26 + offset, 30 + offset, "ESCS", "short_form");
		DefaultComplexSlotMention csm = new DefaultComplexSlotMention("has_short_form");
		csm.addClassMention(abbrevAnnot2.getClassMention());
		abbrevAnnot1.getClassMention().addComplexSlotMention(csm);

		TextAnnotation abbrevAnnot3 = factory.createAnnotation(147 + offset, 164 + offset, "electroretinogram",
				"long_form");
		TextAnnotation abbrevAnnot4 = factory.createAnnotation(166 + offset, 169 + offset, "ERG", "short_form");
		DefaultComplexSlotMention csm2 = new DefaultComplexSlotMention("has_short_form");
		csm2.addClassMention(abbrevAnnot4.getClassMention());
		abbrevAnnot3.getClassMention().addComplexSlotMention(csm2);

		// check that the abbreviation spans are correct
		assertEquals(abbrevAnnot1.getCoveredText(),
				documentText.substring(abbrevAnnot1.getAnnotationSpanStart(), abbrevAnnot1.getAnnotationSpanEnd()));
		assertEquals(abbrevAnnot2.getCoveredText(),
				documentText.substring(abbrevAnnot2.getAnnotationSpanStart(), abbrevAnnot2.getAnnotationSpanEnd()));

		abbrevAnnots.add(abbrevAnnot1);
		abbrevAnnots.add(abbrevAnnot3);

		Collection<TextAnnotation> sentenceAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(sentenceAnnot1, sentenceAnnot2, sentenceAnnot3));

		String augmentedDocumentText = DocumentTextAugmentationFn.augmentDocumentText(documentText, abbrevAnnots,
				sentenceAnnots);

		String expectedAugSent2aMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentenceAnnot2.getAnnotationSpanStart(),
				abbrevAnnot1.getAnnotationSpanStart(), abbrevAnnot2.getAnnotationSpanEnd());

		String expectedAugSent2bMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentenceAnnot2.getAnnotationSpanStart(),
				abbrevAnnot3.getAnnotationSpanStart(), abbrevAnnot4.getAnnotationSpanEnd());

		String expectedAugSent2aText = "Enhanced S-cone syndrome        is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light.";
		String expectedAugSent2bText = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram       with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light.";
		String expectedAugDocText = String.format("%s\n%s\n%s\n%s\n%s\n%s\n", documentText,
				UtilityOgerDictFileFactory.DOCUMENT_END_MARKER, expectedAugSent2aMetadataLine, expectedAugSent2aText,
				expectedAugSent2bMetadataLine, expectedAugSent2bText);

		System.out.println("AUGMENTED:\n" + augmentedDocumentText);

		assertEquals(expectedAugDocText, augmentedDocumentText);
	@Test
	public void testGetAugmentedDocumentTextAndSentenceBionlp_debugStringOOBError() throws IOException {

		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMID14691534.txt", ENCODING);

		String sentenceBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"PMID14691534.sentences.bionlp", ENCODING);
		String abbreviationBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"PMID14691534.abbreviations.bionlp", ENCODING);

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument sentenceDoc = bionlpReader.readDocument("14691534", "CRAFT",
				new ByteArrayInputStream(sentenceBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument abbreviationDoc = bionlpReader.readDocument("14691534", "CRAFT",
				new ByteArrayInputStream(abbreviationBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);

		String[] augmentedDocTextSentBionlp = DocumentTextAugmentationFn.getAugmentedDocumentTextAndSentenceBionlp(
				docText, abbreviationDoc.getAnnotations(), sentenceDoc.getAnnotations());

	}

}
