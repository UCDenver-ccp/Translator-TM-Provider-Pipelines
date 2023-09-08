package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationUtil;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultComplexSlotMention;

public class DocumentTextAugmentationFnTest {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	@Test
	public void testGetAugmentedDocumentTextAndSentenceBionlp() throws IOException {

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

		String[] augmentedDocTextSentBionlp = DocumentTextAugmentationFn
				.getAugmentedDocumentTextAndSentenceBionlp(documentText, abbrevAnnots, sentenceAnnots);

		String augmentedDocumentText = augmentedDocTextSentBionlp[0];

		String expectedAugSent2aMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentenceAnnot2.getAnnotationSpanStart(),
				abbrevAnnot1.getAnnotationSpanStart(), abbrevAnnot2.getAnnotationSpanEnd());

		String expectedAugSent2bMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentenceAnnot2.getAnnotationSpanStart(),
				abbrevAnnot3.getAnnotationSpanStart(), abbrevAnnot4.getAnnotationSpanEnd());

		String expectedAugSent2aText = "Enhanced S-cone syndrome        is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light.";
		String expectedAugSent2bText = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram       with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light.";
		String expectedAugDocText = String.format("\n%s\n%s\n%s\n%s\n%s\n",
				UtilityOgerDictFileFactory.DOCUMENT_END_MARKER, expectedAugSent2aMetadataLine, expectedAugSent2aText,
				expectedAugSent2bMetadataLine, expectedAugSent2bText);

//		System.out.println("doc text length: " + documentText.length());
//		System.out.println("AUGMENTED:" + augmentedDocumentText);

		assertEquals(expectedAugDocText, augmentedDocumentText);

		String augmentedSentBionlp = augmentedDocTextSentBionlp[1];

		int augSent2aStart = documentText.length() + 1 + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER.length() + 1
				+ expectedAugSent2aMetadataLine.length() + 1;
		int augSent2aEnd = augSent2aStart + expectedAugSent2aText.length();

		int augSent2bStart = augSent2aEnd + 1 + expectedAugSent2bMetadataLine.length() + 1;
		int augSent2bEnd = augSent2bStart + expectedAugSent2bText.length();

		String expectedAugSentBionlp = String.format("T4\taugmented_sentence %d %d\t%s\n", augSent2aStart, augSent2aEnd,
				expectedAugSent2aText)
				+ String.format("T5\taugmented_sentence %d %d\t%s\n", augSent2bStart, augSent2bEnd,
						expectedAugSent2bText);

		assertEquals(expectedAugSentBionlp, augmentedSentBionlp);

	}

	/**
	 * testing that no exception is thrown
	 * 
	 * @throws IOException
	 */
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

	/**
	 * testing that no exception is thrown
	 * 
	 * This OOB turned out to be an abbreviation error. Still a good test.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetAugmentedDocumentTextAndSentenceBionlp_abbrevAtBeginningOfDoc() throws IOException {
		// 012345678901
		String docText = "Nickel (Ni) phytotoxicity and detoxification mechanisms: A review";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("1234");
		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, docText.length(), docText, "sentence");
		List<TextAnnotation> sentenceAnnots = Arrays.asList(sentenceAnnot1);

		TextAnnotation lfAbbrev = factory.createAnnotation(0, 6, "Nickel", AbbreviationFn.LONG_FORM_TYPE);
		TextAnnotation sfAbbrev = factory.createAnnotation(8, 10, "Ni", AbbreviationFn.SHORT_FORM_TYPE);
		TextAnnotationUtil.addSlotValue(lfAbbrev, AbbreviationFn.HAS_SHORT_FORM_RELATION, sfAbbrev.getClassMention());

		List<TextAnnotation> abbrevAnnots = Arrays.asList(lfAbbrev);

		String[] augmentedDocTextSentBionlp = DocumentTextAugmentationFn
				.getAugmentedDocumentTextAndSentenceBionlp(docText, abbrevAnnots, sentenceAnnots);

	}

	/**
	 * testing the non-canonical case where the long form of an abbreviation appears
	 * in parens after the short form
	 * 
	 * <pre>
	 * T1	short_form 0 6	TRIM29
	T2	long_form 8 38	Tripartite Motif Containing 29
	T3	short_form 51 56	NLRC4
	T4	long_form 58 101	NLR Family CARD Domain Containing Protein 4
	 * </pre>
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetAugmentedDocumentTextAndSentenceBionlp_abbrevAtBeginningOfDoc2() throws IOException {
		// 012345678901
		String docText = "TRIM29 (Tripartite Motif Containing 29) Alleviates NLRC4 (NLR Family CARD Domain Containing Protein 4) Inflammasome Related Cerebral Injury via Promoting Proteasomal Degradation of NLRC4 in Ischemic Stroke.";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("1234");
		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, docText.length(), docText, "sentence");
		List<TextAnnotation> sentenceAnnots = Arrays.asList(sentenceAnnot1);

		TextAnnotation sfAbbrev1 = factory.createAnnotation(0, 6, "TRIM29", AbbreviationFn.SHORT_FORM_TYPE);
		TextAnnotation lfAbbrev1 = factory.createAnnotation(8, 38, "Tripartite Motif Containing 29",
				AbbreviationFn.LONG_FORM_TYPE);
		TextAnnotation sfAbbrev2 = factory.createAnnotation(51, 56, "NLRC4", AbbreviationFn.SHORT_FORM_TYPE);
		TextAnnotation lfAbbrev2 = factory.createAnnotation(58, 101, "NLR Family CARD Domain Containing Protein 4",
				AbbreviationFn.LONG_FORM_TYPE);
		TextAnnotationUtil.addSlotValue(lfAbbrev1, AbbreviationFn.HAS_SHORT_FORM_RELATION, sfAbbrev1.getClassMention());
		TextAnnotationUtil.addSlotValue(lfAbbrev2, AbbreviationFn.HAS_SHORT_FORM_RELATION, sfAbbrev2.getClassMention());

		List<TextAnnotation> abbrevAnnots = Arrays.asList(lfAbbrev1, lfAbbrev2);

		String[] augmentedDocTextSentBionlp = DocumentTextAugmentationFn
				.getAugmentedDocumentTextAndSentenceBionlp(docText, abbrevAnnots, sentenceAnnots);

		String augmentedDocumentText = augmentedDocTextSentBionlp[0];

		int sentSpanStart = 0;
		String expectedAugSent1aMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentSpanStart,
				sfAbbrev1.getAnnotationSpanStart(), lfAbbrev1.getAnnotationSpanEnd());

		String expectedAugSent1bMetadataLine = String.format("%s\t%d\t%d\t%d",
				DocumentTextAugmentationFn.AUGMENTED_SENTENCE_INDICATOR, sentSpanStart,
				sfAbbrev2.getAnnotationSpanStart(), lfAbbrev2.getAnnotationSpanEnd());

		String expectedAugSent1aText = "        Tripartite Motif Containing 29  Alleviates NLRC4 (NLR Family CARD Domain Containing Protein 4) Inflammasome Related Cerebral Injury via Promoting Proteasomal Degradation of NLRC4 in Ischemic Stroke.";
		String expectedAugSent1bText = "TRIM29 (Tripartite Motif Containing 29) Alleviates        NLR Family CARD Domain Containing Protein 4  Inflammasome Related Cerebral Injury via Promoting Proteasomal Degradation of NLRC4 in Ischemic Stroke.";
		String expectedAugDocText = String.format("\n%s\n%s\n%s\n%s\n%s\n",
				UtilityOgerDictFileFactory.DOCUMENT_END_MARKER, expectedAugSent1aMetadataLine, expectedAugSent1aText,
				expectedAugSent1bMetadataLine, expectedAugSent1bText);

		assertEquals(expectedAugDocText, augmentedDocumentText);

		String augmentedSentBionlp = augmentedDocTextSentBionlp[1];

		int augSent2aStart = docText.length() + 1 + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER.length() + 1
				+ expectedAugSent1aMetadataLine.length() + 1;
		int augSent2aEnd = augSent2aStart + expectedAugSent1aText.length();

		int augSent2bStart = augSent2aEnd + 1 + expectedAugSent1bMetadataLine.length() + 1;
		int augSent2bEnd = augSent2bStart + expectedAugSent1bText.length();

		String expectedAugSentBionlp = String.format("T2\taugmented_sentence %d %d\t%s\n", augSent2aStart, augSent2aEnd,
				expectedAugSent1aText)
				+ String.format("T3\taugmented_sentence %d %d\t%s\n", augSent2bStart, augSent2bEnd,
						expectedAugSent1bText);

		assertEquals(expectedAugSentBionlp, augmentedSentBionlp);

	}

	/**
	 * testing that no exception is thrown
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetAugmentedDocumentTextAndSentenceBionlp_debugStringOOBError2() throws IOException {

		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "PMID37021569.txt", ENCODING);

		String sentenceBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"PMID37021569.sentences.bionlp", ENCODING);
		String abbreviationBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"PMID37021569.abbreviations.bionlp", ENCODING);

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument sentenceDoc = bionlpReader.readDocument("37021569", "CRAFT",
				new ByteArrayInputStream(sentenceBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument abbreviationDoc = bionlpReader.readDocument("37021569", "CRAFT",
				new ByteArrayInputStream(abbreviationBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);

		String[] augmentedDocTextSentBionlp = DocumentTextAugmentationFn.getAugmentedDocumentTextAndSentenceBionlp(
				docText, abbreviationDoc.getAnnotations(), sentenceDoc.getAnnotations());

	}

}
