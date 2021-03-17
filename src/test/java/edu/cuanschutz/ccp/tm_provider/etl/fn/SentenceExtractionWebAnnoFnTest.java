package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SentenceExtractionWebAnnoFn.SimpleTokenizer;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;

//@Ignore("causes inconsistent ConcurrentModificationException when run -- unsure why this happens")
public class SentenceExtractionWebAnnoFnTest {

	private static final String PLACEHOLDER_X = "@CONCEPTX$";
	private static final String PLACEHOLDER_Y = "@CONCEPTY$";

	private static final String Y_000001 = "Y:000001";
	private static final String X_000001 = "X:000001";
	private static final String X_000002 = "X:000002";
	private List<TextAnnotation> sentenceAnnotations;
	private List<TextAnnotation> conceptXAnnots;
	private List<TextAnnotation> conceptYAnnots;
	// 1 2 3 4
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence1 = "This sentence has conceptX1 and conceptX2.";
	// sentence: [0 42] conceptX1 [18 27] conceptX2 [32 41]

	// 4 5 6 7 8 9
	// 0123456789012345678901234567890123456789012345678901234
	private static final String sentence2 = "ConceptX1 is in this sentence, and so is conceptY1.";
	// sentence: [43 94] conceptX1 [43 52] conceptY1 [84 93]

	// 0 1 2 3
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence3 = "There are no concepts in this sentence.";
	// sentence: [95 134]

	// 4 5 6 4
	// 012345678901234567890123456789012345678901234567890123456789
	private static final String sentence4 = "ConceptX1 is in this sentence.";
	// sentence: [135 165] conceptX1 [135 144]

	private static final String documentText = sentence1 + " " + sentence2 + " " + sentence3 + " " + sentence4;
	private static final String documentId = "PMID:12345";

	private static final String SENTENCE = "sentence";

	private static TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(documentId);

	private TextAnnotation x1Sentence1Annot;
	private TextAnnotation x2Sentence1Annot;
	private TextAnnotation x1Sentence2Annot;
	private TextAnnotation x1Sentence4Annot;
	private TextAnnotation y1Sentence2Annot;

	// This is simulating the concept Y_000001 existing in both the X & Y
	// ontologies, e.g. an extension class
	private TextAnnotation x3ReallyY1Sentence2Annot;

	private TextAnnotation sentence1Annot;
	private TextAnnotation sentence2Annot;
	private TextAnnotation sentence3Annot;
	private TextAnnotation sentence4Annot;

	@Before
	public void setUp() {
		x1Sentence1Annot = factory.createAnnotation(18, 27, "conceptX1", X_000001);
		x2Sentence1Annot = factory.createAnnotation(32, 41, "conceptX2", X_000002);
		x1Sentence2Annot = factory.createAnnotation(43, 52, "ConceptX1", X_000001);
		x1Sentence4Annot = factory.createAnnotation(135, 144, "ConceptX1", X_000001);
		y1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

		x3ReallyY1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

		sentence1Annot = factory.createAnnotation(0, 42, sentence1, SENTENCE);
		sentence2Annot = factory.createAnnotation(43, 94, sentence2, SENTENCE);
		sentence3Annot = factory.createAnnotation(95, 134, sentence3, SENTENCE);
		sentence4Annot = factory.createAnnotation(135, 165, sentence4, SENTENCE);

		sentenceAnnotations = populateSentenceAnnotations();
		conceptXAnnots = populateXConceptAnnotations();
		conceptYAnnots = populateYConceptAnnotations();
	}

	private List<TextAnnotation> populateXConceptAnnotations() {
		List<TextAnnotation> conceptXAnnots = new ArrayList<TextAnnotation>();
		conceptXAnnots.add(x1Sentence1Annot);
		conceptXAnnots.add(x2Sentence1Annot);
		conceptXAnnots.add(x1Sentence2Annot);
		conceptXAnnots.add(x1Sentence4Annot);
		return conceptXAnnots;
	}

	private List<TextAnnotation> populateYConceptAnnotations() {
		List<TextAnnotation> conceptYAnnots = new ArrayList<TextAnnotation>();
		conceptYAnnots.add(y1Sentence2Annot);
		return conceptYAnnots;

	}

	private List<TextAnnotation> populateSentenceAnnotations() {
		List<TextAnnotation> sentenceAnnotations = new ArrayList<TextAnnotation>();
		sentenceAnnotations.add(sentence1Annot);
		sentenceAnnotations.add(sentence2Annot);
		sentenceAnnotations.add(sentence3Annot);
		sentenceAnnotations.add(sentence4Annot);
		return sentenceAnnotations;
	}

//	@Test
//	public void testBuildSentenceToConceptMap() {
//		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentToConceptMap = SentenceExtractionWebAnnoFn
//				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);
//
//		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> expectedSentToConceptMap = new HashMap<TextAnnotation, Map<String, Set<TextAnnotation>>>();
//		Map<String, Set<TextAnnotation>> map1 = new HashMap<String, Set<TextAnnotation>>();
//		map1.put(SentenceExtractionWebAnnoFn.X_CONCEPTS,
//				CollectionsUtil.createSet(x1Sentence1Annot, x2Sentence1Annot));
//		expectedSentToConceptMap.put(sentence1Annot, map1);
//
//		Map<String, Set<TextAnnotation>> map2 = new HashMap<String, Set<TextAnnotation>>();
//		map2.put(SentenceExtractionWebAnnoFn.X_CONCEPTS, CollectionsUtil.createSet(x1Sentence2Annot));
//		map2.put(SentenceExtractionWebAnnoFn.Y_CONCEPTS, CollectionsUtil.createSet(y1Sentence2Annot));
//		expectedSentToConceptMap.put(sentence2Annot, map2);
//
//		Map<String, Set<TextAnnotation>> map4 = new HashMap<String, Set<TextAnnotation>>();
//		map4.put(SentenceExtractionWebAnnoFn.X_CONCEPTS, CollectionsUtil.createSet(x1Sentence4Annot));
//		expectedSentToConceptMap.put(sentence4Annot, map4);
//
//		assertEquals(expectedSentToConceptMap, sentToConceptMap);
//
//	}

//	@Test
//	public void testSentenceContainsKeyword() {
//
//		String sentence = "This sentence discusses increasing and decreasing rates.";
//		String keyword = SentenceExtractionWebAnnoFn.sentenceContainsKeyword(sentence,
//				CollectionsUtil.createSet("increasing"));
//		String expectedKeyword = "increasing";
//		assertEquals(expectedKeyword, keyword);
//
//		keyword = SentenceExtractionWebAnnoFn.sentenceContainsKeyword(sentence,
//				CollectionsUtil.createSet("DECREasing"));
//		expectedKeyword = "DECREasing";
//		assertEquals(expectedKeyword, keyword);
//
//		keyword = SentenceExtractionWebAnnoFn.sentenceContainsKeyword(sentence,
//				CollectionsUtil.createSet("notfound"));
//		assertNull(keyword);
//
//		keyword = SentenceExtractionWebAnnoFn.sentenceContainsKeyword(sentence,
//				CollectionsUtil.createSet("increas"));
//		assertNull(keyword);
//
//	}

//	@Test
//	public void testFilterViaCrf() {
//		List<TextAnnotation> crfAnnots = new ArrayList<TextAnnotation>();
//		crfAnnots.add(factory.createAnnotation(32, 41, "conceptX2", X_000002));
//		crfAnnots.add(factory.createAnnotation(49, 55, "ConceptX1", X_000001));
//
//		List<TextAnnotation> expectedAnnots = new ArrayList<TextAnnotation>();
//		expectedAnnots.add(factory.createAnnotation(32, 41, "conceptX2", X_000002));
//		expectedAnnots.add(factory.createAnnotation(43, 52, "ConceptX1", X_000001));
//
//		List<TextAnnotation> filteredAnnots = PipelineMain.filterViaCrf(conceptXAnnots, crfAnnots);
//
//		assertEquals(expectedAnnots, filteredAnnots);
//	}

	@Test
	public void testCatalogExtractedSentences() {
		Set<String> keywords = CollectionsUtil.createSet("sentence");

		Set<String> extractedSentences = extractSentences(keywords);

		String expectedSentence = "\n#Text=ConceptX1 is in this sentence, and so is conceptY1.\n"
				+ "1-1	0-9	ConceptX1	@CONCEPTX$\n" + "1-2	10-12	is	_\n" + "1-3	13-15	in	_\n"
				+ "1-4	16-20	this	_\n" + "1-5	21-29	sentence	_\n" + "1-6	29-30	,	_\n"
				+ "1-7	31-34	and	_\n" + "1-8	35-37	so	_\n" + "1-9	38-40	is	_\n"
				+ "1-10	41-50	conceptY1	@CONCEPTY$\n" + "1-11	50-51	.	_\n";

		Set<String> expectedExtractedSentences = new HashSet<String>();
		expectedExtractedSentences.add(expectedSentence);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	/**
	 * There are cases, e.g. extension classes, where the same ontology concepts
	 * exists in different ontologies. Extracted sentences should not link the same
	 * concept, e.g. concept x should not equal concept y.
	 */
	@Ignore("Ignoring for now. Not quite sure how to handle this case. It should result in an overlapping annotation.")
	@Test
	public void testCatalogExtractedSentencesPreventDuplicates() {
		Set<String> keywords = CollectionsUtil.createSet("sentence");

		// this is concept X but is part of the y ontology so it potentially creates a
		// situation where an ExtractedSentence contains two references to concept X1.
		conceptXAnnots.add(x3ReallyY1Sentence2Annot);

		Set<String> extractedSentences = extractSentences(keywords);

		// @formatter:off
		String expectedSentence = "\n#Text=ConceptX1 is in this sentence, and so is conceptY1.\n" + 
				"1-1	0-9	ConceptX1	@CONCEPTX$\n" + 
				"1-2	10-12	is	_\n" + 
				"1-3	13-15	in	_\n" + 
				"1-4	16-20	this	_\n" + 
				"1-5	21-29	sentence	_\n" + 
				"1-6	29-30	,	_\n" + 
				"1-7	31-34	and	_\n" + 
				"1-8	35-37	so	_\n" + 
				"1-9	38-40	is	_\n" + 
				"1-10	41-50	conceptY1	@CONCEPTY$\n" + 
				"1-11	50-51	.	_\n";
		// @formatter:on

		Set<String> expectedExtractedSentences = new HashSet<String>();
		expectedExtractedSentences.add(expectedSentence);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	private Set<String> extractSentences(Set<String> keywords) {
		SimpleTokenizer tokenizer = new SimpleTokenizer();
		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionConceptAllFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);

		Set<String> extractedSentences = SentenceExtractionWebAnnoFn.catalogExtractedSentences(keywords, documentText,
				documentId, sentenceToConceptMap, PLACEHOLDER_X, PLACEHOLDER_Y, tokenizer);
		return extractedSentences;
	}

	@Test
	public void testCatalogExtractedSentencesNoKeyword() {
		Set<String> keywords = null;

		Set<String> extractedSentences = extractSentences(keywords);

		// @formatter:off
		String expectedSentence = "\n#Text=ConceptX1 is in this sentence, and so is conceptY1.\n" + 
				"1-1	0-9	ConceptX1	@CONCEPTX$\n" + 
				"1-2	10-12	is	_\n" + 
				"1-3	13-15	in	_\n" + 
				"1-4	16-20	this	_\n" + 
				"1-5	21-29	sentence	_\n" + 
				"1-6	29-30	,	_\n" + 
				"1-7	31-34	and	_\n" + 
				"1-8	35-37	so	_\n" + 
				"1-9	38-40	is	_\n" + 
				"1-10	41-50	conceptY1	@CONCEPTY$\n" + 
				"1-11	50-51	.	_\n";
		// @formatter:on

		Set<String> expectedExtractedSentences = new HashSet<String>();
		expectedExtractedSentences.add(expectedSentence);
		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	/**
	 * There may be cases where we want to extract sentences that contain two of the
	 * same type, e.g. for PR - regulates - PR.
	 */
	@Test
	public void testCatalogExtractedSentencesNoKeyword_DuplicatePlaceholder() {
		SimpleTokenizer tokenizer = new SimpleTokenizer();
		Set<String> keywords = null;

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionConceptAllFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptXAnnots);

		Set<String> extractedSentences = SentenceExtractionWebAnnoFn.catalogExtractedSentences(keywords, documentText,
				documentId, sentenceToConceptMap, PLACEHOLDER_X, PLACEHOLDER_X, tokenizer);

		for (String s : extractedSentences) {
			System.out.println("----------" + s);
		}

		// @formatter:off
		String extractedSentence = "\n#Text=This sentence has conceptX1 and conceptX2.\n" + 
				"1-1	0-4	This	_\n" + 
				"1-2	5-13	sentence	_\n" + 
				"1-3	14-17	has	_\n" + 
				"1-4	18-27	conceptX1	@CONCEPTX$\n" + 
				"1-5	28-31	and	_\n" + 
				"1-6	32-41	conceptX2	@CONCEPTX$\n" + 
				"1-7	41-42	.	_\n";
		// @formatter:on

		Set<String> expectedExtractedSentences = new HashSet<String>();
		expectedExtractedSentences.add(extractedSentence);

		assertEquals(expectedExtractedSentences.size(), extractedSentences.size());
		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testAnnotationHash() {
		TextAnnotation annot1 = factory.createAnnotation(0, 5, "hello", new DefaultClassMention("greeting"));
		TextAnnotation annot2 = factory.createAnnotation(0, 5, "hello", new DefaultClassMention("greeting"));
		TextAnnotation annot3 = factory.createAnnotation(Arrays.asList(new Span(0, 5)), "hello",
				new DefaultClassMention("greeting"));

		assertEquals(annot1.hashCode(), annot2.hashCode());
		assertEquals(annot1.hashCode(), annot3.hashCode());

		Set<TextAnnotation> annots = new HashSet<TextAnnotation>();
		annots.add(annot1);
		annots.add(annot2);
		annots.add(annot3);

		assertEquals(1, annots.size());

	}

	@Test
	public void testCatalogExtractedSentencesKeywordNotFound() {
		Set<String> keywords = CollectionsUtil.createSet("notfound");
		Set<String> extractedSentences = extractSentences(keywords);

		// keyword id not found, so no sentences are extracted
		Set<String> expectedExtractedSentences = new HashSet<String>();

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testExtractSentences() throws IOException {

		DocumentCriteria textDc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria sentenceDc = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0");
		DocumentCriteria conceptAllDc = new DocumentCriteria(DocumentType.CONCEPT_ALL, DocumentFormat.BIONLP,
				PipelineKey.CONCEPT_POST_PROCESS, "0.1.0");

		ProcessingStatus status = new ProcessingStatus(documentId);
		status.addCollection("PUBMED");
		status.addCollection("PUBMED_SUB_0");

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.SENTENCE_DONE);
		status.enableFlag(ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		String sentenceBionlp = null;
//
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(sentenceAnnotations);
			writer.serialize(td, outputStream, encoding);
			sentenceBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(y1Sentence2Annot);
		conceptAnnots.add(x2Sentence1Annot);
		conceptAnnots.add(x1Sentence2Annot);

		String conceptAnnotBionlp = null;

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(conceptAnnots);
			writer.serialize(td, outputStream, encoding);
			conceptAnnotBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();
		map.put(textDc, documentText);
		map.put(sentenceDc, sentenceBionlp);
		map.put(conceptAllDc, conceptAnnotBionlp);

		// the word sentence appears in all of the sentences
		Set<String> keywords = CollectionsUtil.createSet("sentence");
		Map<String, String> suffixToPlaceholderMap = new HashMap<String, String>();
		suffixToPlaceholderMap.put("X", PLACEHOLDER_X);
		suffixToPlaceholderMap.put("Y", PLACEHOLDER_Y);

		SimpleTokenizer tokenizer = new SimpleTokenizer();

		{
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
					.getDocTypeToContentMap(documentId, map);

			Set<String> extractedSentences = SentenceExtractionWebAnnoFn.extractSentences(documentId, documentText,
					docTypeToContentMap, keywords, suffixToPlaceholderMap, tokenizer);
			assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		}

		{
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
					.getDocTypeToContentMap(documentId, map);

			// no keywords
			keywords = null;
			Set<String> extractedSentences = SentenceExtractionWebAnnoFn.extractSentences(documentId, documentText,
					docTypeToContentMap, keywords, suffixToPlaceholderMap, tokenizer);
			assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		}

		{
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
					.getDocTypeToContentMap(documentId, map);

			// no keywords
			keywords = new HashSet<String>();
			Set<String> extractedSentences = SentenceExtractionWebAnnoFn.extractSentences(documentId, documentText,
					docTypeToContentMap, keywords, suffixToPlaceholderMap, tokenizer);
			assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());

		}

		{
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
					.getDocTypeToContentMap(documentId, map);

			// keyword not found so no sentence extracted
			keywords = CollectionsUtil.createSet("notfound");
			Set<String> extractedSentences = SentenceExtractionWebAnnoFn.extractSentences(documentId, documentText,
					docTypeToContentMap, keywords, suffixToPlaceholderMap, tokenizer);
			assertEquals("there should be no extracted sentences", 0, extractedSentences.size());
		}
	}

}
