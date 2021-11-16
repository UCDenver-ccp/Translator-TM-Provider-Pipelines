package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
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

public class SentenceExtractionFnTest {

	/*
	 * TODO: determining which document zone to assign to sentences is not yet
	 * implemented, so it will always return null for now
	 */
	private static final String DOCUMENT_ZONE = "introduction";
	private static final int DOCUMENT_YEAR_PUBLISHED = 2021;
	private static final Set<String> DOCUMENT_PUBLICATION_TYPES = CollectionsUtil.createSet("Journal Article");

	private static final String PLACEHOLDER_X = "@CONCEPTX$";
	private static final String PLACEHOLDER_Y = "@CONCEPTY$";

	private static final String Y_000001 = "Y:000001";
	private static final String X_000001 = "X:000001";
	private static final String X_000001_SYN = "X:000001_SYN";
	private static final String X_000002 = "X:000002";
	private static final String X_000003 = "X:000003";
	private static final String X_000004 = "X:000004";
	private static final String X_000005 = "X:000005";
	private static final String X_000006 = "X:000006";

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

//	private TextAnnotation x1Sentence1Annot = createX1Sentence1Annot();
	private static Span x1Sentence1Span = new Span(18, 27);

	private static TextAnnotation createX1Sentence1Annot() {
		return factory.createAnnotation(x1Sentence1Span.getSpanStart(), x1Sentence1Span.getSpanEnd(), "conceptX1",
				X_000001);
	}

//	private TextAnnotation x2Sentence1Annot = createX2Sentence1Annot();
	private static Span x2Sentence1Span = new Span(32, 41);

	private static TextAnnotation createX2Sentence1Annot() {
		return factory.createAnnotation(x2Sentence1Span.getSpanStart(), x2Sentence1Span.getSpanEnd(), "conceptX2",
				X_000002);
	}

//	private static TextAnnotation x1Sentence2Annot = createX1Sentence2Annot();
	private static Span x1Sentence2Span = new Span(43 - 43, 52 - 43);

	private static TextAnnotation createX1Sentence2Annot() {
		return factory.createAnnotation(43, 52, "ConceptX1", X_000001);
	}

//	private static TextAnnotation x1Sentence2SynonymAnnot = createX1Sentence2SynonymAnnot();

	private static TextAnnotation createX1Sentence2SynonymAnnot() {
		return factory.createAnnotation(43, 52, "ConceptX1", X_000001_SYN);
	}

//	private static TextAnnotation x1Sentence4Annot = createX1Sentence4Annot();

	private static TextAnnotation createX1Sentence4Annot() {
		return factory.createAnnotation(135, 144, "ConceptX1", X_000001);
	}

//	private static Span x1Sentence4Span = new Span(135 - 135, 144 - 135);
	private static Span y1Sentence2Span = new Span(84 - 43, 93 - 43);
//	private static TextAnnotation y1Sentence2Annot = createY1Sentence2Annot();

	private static TextAnnotation createY1Sentence2Annot() {
		return factory.createAnnotation(84, 93, "conceptY1", Y_000001);
	}

	// This is simulating the concept Y_000001 existing in both the X & Y
	// ontologies, e.g. an extension class
	private static TextAnnotation x3ReallyY1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

//	private TextAnnotation sentence1Annot = createSentence1Annot();

	private TextAnnotation createSentence1Annot() {
		return factory.createAnnotation(0, 42, sentence1, SENTENCE);
	}

//	private TextAnnotation sentence2Annot = createSentence2Annot();

	private TextAnnotation createSentence2Annot() {
		return factory.createAnnotation(43, 94, sentence2, SENTENCE);
	}

//	private TextAnnotation sentence3Annot = createSentence3Annot();

	private TextAnnotation createSentence3Annot() {
		return factory.createAnnotation(95, 134, sentence3, SENTENCE);
	}

//	private TextAnnotation sentence4Annot = createSentence4Annot();

	private TextAnnotation createSentence4Annot() {
		return factory.createAnnotation(135, 165, sentence4, SENTENCE);
	}

	private List<TextAnnotation> populateXConceptAnnotations() {
		List<TextAnnotation> conceptXAnnots = new ArrayList<TextAnnotation>();
		conceptXAnnots.add(createX1Sentence1Annot());
		conceptXAnnots.add(createX2Sentence1Annot());
		conceptXAnnots.add(createX1Sentence2Annot());
		conceptXAnnots.add(createX1Sentence4Annot());
		return conceptXAnnots;
	}

	private List<TextAnnotation> populateYConceptAnnotations() {
		List<TextAnnotation> conceptYAnnots = new ArrayList<TextAnnotation>();
		conceptYAnnots.add(createY1Sentence2Annot());
		return conceptYAnnots;

	}

	private List<TextAnnotation> populateSentenceAnnotations() {
		List<TextAnnotation> sentenceAnnotations = new ArrayList<TextAnnotation>();
		sentenceAnnotations.add(createSentence1Annot());
		sentenceAnnotations.add(createSentence2Annot());
		sentenceAnnotations.add(createSentence3Annot());
		sentenceAnnotations.add(createSentence4Annot());
		return sentenceAnnotations;
	}

	@Test
	public void testBuildSentenceToConceptMap() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentToConceptMap = SentenceExtractionFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> expectedSentToConceptMap = new HashMap<TextAnnotation, Map<String, Set<TextAnnotation>>>();
		Map<String, Set<TextAnnotation>> map1 = new HashMap<String, Set<TextAnnotation>>();
		map1.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(createX1Sentence1Annot(), createX2Sentence1Annot()));
		expectedSentToConceptMap.put(createSentence1Annot(), map1);

		Map<String, Set<TextAnnotation>> map2 = new HashMap<String, Set<TextAnnotation>>();
		map2.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(createX1Sentence2Annot()));
		map2.put(SentenceExtractionFn.Y_CONCEPTS, CollectionsUtil.createSet(createY1Sentence2Annot()));
		expectedSentToConceptMap.put(createSentence2Annot(), map2);

		Map<String, Set<TextAnnotation>> map4 = new HashMap<String, Set<TextAnnotation>>();
		map4.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(createX1Sentence4Annot()));
		expectedSentToConceptMap.put(createSentence4Annot(), map4);

		assertEquals(expectedSentToConceptMap, sentToConceptMap);

	}

	@Test
	public void testSentenceContainsKeyword() {

		String sentence = "This sentence discusses increasing and decreasing rates.";
		String keyword = SentenceExtractionFn.sentenceContainsKeyword(sentence,
				CollectionsUtil.createSet("increasing"));
		String expectedKeyword = "increasing";
		assertEquals(expectedKeyword, keyword);

		keyword = SentenceExtractionFn.sentenceContainsKeyword(sentence, CollectionsUtil.createSet("DECREasing"));
		expectedKeyword = "DECREasing";
		assertEquals(expectedKeyword, keyword);

		keyword = SentenceExtractionFn.sentenceContainsKeyword(sentence, CollectionsUtil.createSet("notfound"));
		assertNull(keyword);

		keyword = SentenceExtractionFn.sentenceContainsKeyword(sentence, CollectionsUtil.createSet("increas"));
		assertNull(keyword);

	}

	@Test
	public void testFilterViaCrf() {
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();

		List<TextAnnotation> crfAnnots = new ArrayList<TextAnnotation>();
		crfAnnots.add(createX2Sentence1Annot());
		crfAnnots.add(factory.createAnnotation(49, 55, "ConceptX1", X_000001));

		List<TextAnnotation> expectedAnnots = new ArrayList<TextAnnotation>();
		expectedAnnots.add(createX2Sentence1Annot());
		expectedAnnots.add(createX1Sentence2Annot());

		List<TextAnnotation> filteredAnnots = PipelineMain.filterViaCrf(conceptXAnnots, crfAnnots);

		assertEquals(expectedAnnots.size(), filteredAnnots.size());
		assertEquals(expectedAnnots, filteredAnnots);
	}

	@Test
	public void testCatalogExtractedSentences() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));

		Set<String> keywords = CollectionsUtil.createSet("sentence");

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords, sentenceAnnotations, conceptXAnnots,
				conceptYAnnots, sectionAnnots);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, "sentence", sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testCatalogExtractedSentencesWithConceptSynonym() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));

		Set<String> keywords = CollectionsUtil.createSet("sentence");

		conceptXAnnots.add(createX1Sentence2SynonymAnnot());

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords, sentenceAnnotations, conceptXAnnots,
				conceptYAnnots, sectionAnnots);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001 + "|" + X_000001_SYN, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, "sentence", sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	/**
	 * There are cases, e.g. extension classes, where the same ontology concepts
	 * exists in different ontologies. Extracted sentences should not link the same
	 * concept, e.g. concept x should not equal concept y.
	 */
	@Test
	public void testCatalogExtractedSentencesPreventDuplicates() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));

		Set<String> keywords = CollectionsUtil.createSet("sentence");

		// this is concept X but is part of the y ontology so it potentially creates a
		// situation where an ExtractedSentence contains two references to concept X1.
		conceptXAnnots.add(x3ReallyY1Sentence2Annot);

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords, sentenceAnnotations, conceptXAnnots,
				conceptYAnnots, sectionAnnots);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, "sentence", sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	private static Set<ExtractedSentence> extractSentences(Set<String> keywords,
			List<TextAnnotation> sentenceAnnotations, List<TextAnnotation> conceptXAnnots,
			List<TextAnnotation> conceptYAnnots, Collection<TextAnnotation> sectionAnnots) {
		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);

		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.catalogExtractedSentences(keywords,
				documentText, documentId, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED, sentenceToConceptMap,
				PLACEHOLDER_X, PLACEHOLDER_Y, sectionAnnots);
		return extractedSentences;
	}

	@Test
	public void testCatalogExtractedSentencesNoKeyword() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));
		Set<String> keywords = null;

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords, sentenceAnnotations, conceptXAnnots,
				conceptYAnnots, sectionAnnots);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, null, sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	/**
	 * There may be cases where we want to extract sentences that contain two of the
	 * same type, e.g. for PR - regulates - PR.
	 */
	@Test
	public void testCatalogExtractedSentencesNoKeyword_DuplicatePlaceholder() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));
		Set<String> keywords = null;

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptXAnnots);

		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.catalogExtractedSentences(keywords,
				documentText, documentId, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED, sentenceToConceptMap,
				PLACEHOLDER_X, PLACEHOLDER_X, sectionAnnots);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "conceptX1",
				CollectionsUtil.createList(x1Sentence1Span), PLACEHOLDER_X, X_000002, "conceptX2",
				CollectionsUtil.createList(x2Sentence1Span), PLACEHOLDER_X, null, sentence1, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences.size(), extractedSentences.size());
		assertEquals(expectedExtractedSentences.iterator().next().getSentenceIdentifier(),
				extractedSentences.iterator().next().getSentenceIdentifier());
		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testCatalogExtractedSentencesKeywordNotFound() {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> conceptXAnnots = populateXConceptAnnotations();
		List<TextAnnotation> conceptYAnnots = populateYConceptAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));
		Set<String> keywords = CollectionsUtil.createSet("notfound");
		Set<ExtractedSentence> extractedSentences = extractSentences(keywords, sentenceAnnotations, conceptXAnnots,
				conceptYAnnots, sectionAnnots);

		// keyword id not found, so no sentences are extracted
		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testExtractSentences() throws IOException {
		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
		List<TextAnnotation> sectionAnnots = Arrays
				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));

		DocumentCriteria textDc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria sentenceDc = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0");
		DocumentCriteria conceptAllDc = new DocumentCriteria(DocumentType.CONCEPT_ALL, DocumentFormat.BIONLP,
				PipelineKey.CONCEPT_POST_PROCESS, "0.1.0");
		DocumentCriteria sectionDc = new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");

		ProcessingStatus status = new ProcessingStatus(documentId);
		status.addCollection("PUBMED");
		status.addCollection("PUBMED_SUB_0");

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.SENTENCE_DONE);
		status.enableFlag(ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		String sentenceBionlp = null;
		String sectionBionlp = null;
//		String conceptChebiBionlp = null;
//		String crfChebiBionlp = null;
//		String conceptPrBionlp = null;
//		String crfPrBionlp = null;
//
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(sentenceAnnotations);
			writer.serialize(td, outputStream, encoding);
			sentenceBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(sectionAnnots);
			writer.serialize(td, outputStream, encoding);
			sectionBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

//
//		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
//			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
//			td.addAnnotations(conceptXAnnots);
//			writer.serialize(td, outputStream, encoding);
//			conceptChebiBionlp = outputStream.toString(encoding.getCharacterSetName());
//		}
//
//		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
//			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
//			td.addAnnotations(conceptYAnnots);
//			writer.serialize(td, outputStream, encoding);
//			conceptPrBionlp = outputStream.toString(encoding.getCharacterSetName());
//		}
//
//		// the CRF annots overlap with some but not all of the concept annots
//		List<TextAnnotation> crfXAnnots = new ArrayList<TextAnnotation>();
//		crfXAnnots.add(x2Sentence1Annot);
//		crfXAnnots.add(x1Sentence2Annot);
//
//		List<TextAnnotation> crfYAnnots = new ArrayList<TextAnnotation>();
//		crfYAnnots.add(createX2Sentence1Annot());
//
//		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
//			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
//			td.addAnnotations(crfXAnnots);
//			writer.serialize(td, outputStream, encoding);
//			crfChebiBionlp = outputStream.toString(encoding.getCharacterSetName());
//		}
//
//		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
//			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
//			td.addAnnotations(crfYAnnots);
//			writer.serialize(td, outputStream, encoding);
//			crfPrBionlp = outputStream.toString(encoding.getCharacterSetName());
//		}

		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(createY1Sentence2Annot());
		conceptAnnots.add(createX2Sentence1Annot());
		conceptAnnots.add(createX1Sentence2Annot());

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
		map.put(sectionDc, sectionBionlp);

//		KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = KV.of(status, map);

		// the word sentence appears in all of the sentences
		Set<String> keywords = CollectionsUtil.createSet("sentence");
		Map<List<String>, String> suffixToPlaceholderMap = new HashMap<List<String>, String>();
		suffixToPlaceholderMap.put(Arrays.asList("X"), PLACEHOLDER_X);
		suffixToPlaceholderMap.put(Arrays.asList("Y"), PLACEHOLDER_Y);

		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, map);

		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.extractSentences(documentId, documentText,
				DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED, docTypeToContentMap, keywords,
				suffixToPlaceholderMap, DocumentType.CONCEPT_ALL, new HashMap<String, Set<String>>(),
				new HashSet<String>());
		ExtractedSentence esXfirst = new ExtractedSentence(documentId, X_000001, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, "sentence", sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		ExtractedSentence esYfirst = new ExtractedSentence(documentId, Y_000001, "conceptY1",
				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, X_000001, "ConceptX1",
				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, "sentence", sentence2, //documentText,
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);

		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// no keywords
		keywords = null;
		extractedSentences = SentenceExtractionFn.extractSentences(documentId, documentText, DOCUMENT_PUBLICATION_TYPES,
				DOCUMENT_YEAR_PUBLISHED, docTypeToContentMap, keywords, suffixToPlaceholderMap,
				DocumentType.CONCEPT_ALL, new HashMap<String, Set<String>>(), new HashSet<String>());
		esXfirst = new ExtractedSentence(documentId, X_000001, "ConceptX1", CollectionsUtil.createList(x1Sentence2Span),
				PLACEHOLDER_X, Y_000001, "conceptY1", CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, null,
				sentence2,
//				documentText, 
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		esYfirst = new ExtractedSentence(documentId, Y_000001, "conceptY1", CollectionsUtil.createList(y1Sentence2Span),
				PLACEHOLDER_Y, X_000001, "ConceptX1", CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, null,
				sentence2, 
//				documentText, 
				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// no keywords
		keywords = new HashSet<String>();
		extractedSentences = SentenceExtractionFn.extractSentences(documentId, documentText, DOCUMENT_PUBLICATION_TYPES,
				DOCUMENT_YEAR_PUBLISHED, docTypeToContentMap, keywords, suffixToPlaceholderMap,
				DocumentType.CONCEPT_ALL, new HashMap<String, Set<String>>(), new HashSet<String>());
		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// keyword not found so no sentence extracted
		keywords = CollectionsUtil.createSet("notfound");
		extractedSentences = SentenceExtractionFn.extractSentences(documentId, documentText, DOCUMENT_PUBLICATION_TYPES,
				DOCUMENT_YEAR_PUBLISHED, docTypeToContentMap, keywords, suffixToPlaceholderMap,
				DocumentType.CONCEPT_ALL, new HashMap<String, Set<String>>(), new HashSet<String>());
		assertEquals("there should be no extracted sentences", 0, extractedSentences.size());

	}

	@Test
	public void testDetermineDocumentZone() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation titleSentence = factory.createAnnotation(0, 100, "This sentence is in the title.", "sentence");
		TextAnnotation abstractSentence = factory.createAnnotation(110, 150, "This sentence is in the abstract.",
				"sentence");
		TextAnnotation methodsSentence = factory.createAnnotation(450, 550, "This sentence is in the methods section.",
				"sentence");

		TextAnnotation titleSection = factory.createAnnotation(0, 100, "This is the title section.", "title");
		TextAnnotation abstractSection = factory.createAnnotation(102, 200, "This is the abstract section.",
				"abstract");
		TextAnnotation introductionSection = factory.createAnnotation(202, 400, "This is the abstract section.",
				"introduction");
		TextAnnotation methodsSection = factory.createAnnotation(402, 800, "This is the methods section.", "methods");
		TextAnnotation methodsSubSection = factory.createAnnotation(445, 600,
				"This is a subsection in the methods section.", "methods subsection");

		Collection<TextAnnotation> sectionAnnots = Arrays.asList(titleSection, abstractSection, introductionSection,
				methodsSection, methodsSubSection);

		assertEquals("title", SentenceExtractionFn.determineDocumentZone(titleSentence, sectionAnnots));
		assertEquals("abstract", SentenceExtractionFn.determineDocumentZone(abstractSentence, sectionAnnots));
		assertEquals("methods", SentenceExtractionFn.determineDocumentZone(methodsSentence, sectionAnnots));
	}

	@Test
	public void testGetAnnotsByPrefix() {
		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(createY1Sentence2Annot());
		conceptAnnots.add(createX2Sentence1Annot());
		conceptAnnots.add(createX1Sentence2Annot());

		List<String> prefixes = new ArrayList<String>();
		prefixes.add("X");

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		List<TextAnnotation> annots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots, prefixes, ancestorMap);

		List<TextAnnotation> expectedAnnots = Arrays.asList(createX2Sentence1Annot(), createX1Sentence2Annot());

		assertEquals(expectedAnnots, annots);

	}

	@Test
	public void testGetAnnotsByPrefixUseAncestor() {
		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(createY1Sentence2Annot());
		conceptAnnots.add(createX2Sentence1Annot());
		conceptAnnots.add(createX1Sentence2Annot());
		conceptAnnots.add(createX1Sentence4Annot());

		List<String> prefixes = new ArrayList<String>();
		prefixes.add(X_000001);

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put(X_000002, CollectionsUtil.createSet(X_000001));
		List<TextAnnotation> annots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots, prefixes, ancestorMap);

		List<TextAnnotation> expectedAnnots = Arrays.asList(createX2Sentence1Annot(), createX1Sentence2Annot(), createX1Sentence4Annot());

		assertEquals(expectedAnnots.size(), annots.size());
		assertEquals(expectedAnnots, annots);

	}

	@Test
	public void testGetAnnotsByPrefixUseAncestorY() {
		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(createY1Sentence2Annot());
		conceptAnnots.add(createX2Sentence1Annot());
		conceptAnnots.add(createX1Sentence2Annot());
		conceptAnnots.add(createX1Sentence4Annot());

		List<String> prefixes = new ArrayList<String>();
		prefixes.add("Y");

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put(X_000002, CollectionsUtil.createSet(X_000001));
		List<TextAnnotation> annots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots, prefixes, ancestorMap);

		List<TextAnnotation> expectedAnnots = Arrays.asList(createY1Sentence2Annot());

		assertEquals(expectedAnnots.size(), annots.size());
		assertEquals(expectedAnnots, annots);

	}

	@Test
	public void testGetAnnotsByPrefixUseAncestor2() {

		TextAnnotation annot4 = factory.createAnnotation(0, 1, "4", X_000004);
		TextAnnotation annot5 = factory.createAnnotation(10, 11, "5", X_000005);
		TextAnnotation annot6 = factory.createAnnotation(20, 21, "6", X_000006);

		List<TextAnnotation> conceptAnnots = new ArrayList<TextAnnotation>();
		conceptAnnots.add(createY1Sentence2Annot());
		conceptAnnots.add(createX2Sentence1Annot());
		conceptAnnots.add(createX1Sentence2Annot());
		conceptAnnots.add(createX1Sentence4Annot());
		conceptAnnots.add(annot4);
		conceptAnnots.add(annot5);
		conceptAnnots.add(annot6);

		List<String> prefixes = new ArrayList<String>();
		prefixes.add(X_000003);

		Map<String, Set<String>> ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put(X_000002, CollectionsUtil.createSet(X_000001));
		ancestorMap.put(X_000003, CollectionsUtil.createSet(X_000001, X_000002));
		ancestorMap.put(X_000004, CollectionsUtil.createSet(X_000001, X_000002, X_000003));
		ancestorMap.put(X_000005, CollectionsUtil.createSet(X_000001, X_000002, X_000003, X_000004));
		ancestorMap.put(X_000006, CollectionsUtil.createSet(X_000001, X_000002, X_000003, X_000004, X_000005));

		List<TextAnnotation> annots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots, prefixes, ancestorMap);

		List<TextAnnotation> expectedAnnots = Arrays.asList(annot4, annot5, annot6);

//		assertEquals(expectedAnnots.size(), annots.size());
		assertEquals(expectedAnnots, annots);

	}

	@Test
	public void testExtractSentencesRealExample() throws IOException {
		// @formatter:off
		String documentId = "PMID:31111513";
		String documentText = "PTEN loss in prostatic adenocarcinoma correlates with specific adverse histologic features (intraductal carcinoma, cribriform Gleason pattern 4 and stromogenic carcinoma).\n" + 
				"\n" + 
				"The loss of PTEN tumor suppressor gene is one of the most common somatic genetic aberrations in prostate cancer (PCa) and is frequently associated with high-risk disease. Deletion or mutation of at least one PTEN allele has been reported to occur in 20% to 40% of localized PCa and up to 60% of metastases. The goal of this study was to determine if somatic alteration detected by PTEN immunohistochemical loss of expression is associated with specific histologic features.\n" + 
				"Two hundred sixty prostate core needle biopsies with PCa were assessed for PTEN loss using an analytically validated immunohistochemical assay. Blinded to PTEN status, each tumor was assessed for the Grade Group (GG) and the presence or absence of nine epithelial features. Presence of stromogenic PCa was also assessed and defined as grade 3 reactive tumor stroma as previously described: the presence of carcinoma associated stromal response with epithelial to stroma ratio of greater than 50% reactive stroma.\n" + 
				"Eight-eight (34%) cases exhibited PTEN loss while 172 (66%) had intact PTEN. PTEN loss was significantly (P < 0.05) associated with increasing GG, poorly formed glands (74% of total cases with loss vs 49% of intact), and three well-validated unfavorable pathological features: intraductal carcinoma of the prostate (IDC-P) (69% of total cases with loss vs 12% of intact), cribriform Gleason pattern 4 (38% of total cases with loss vs 10% of intact) and stromogenic PCa (23% of total cases with loss vs 6% of intact). IDC-P had the highest relative risk (4.993, 95% confidence interval, 3.451-7.223, P < 0.001) for PTEN loss. At least one of these three unfavorable pathological features were present in 67% of PCa exhibiting PTEN loss, while only 11% of PCa exhibited PTEN loss when none of these three unfavorable pathological features were present.\n" + 
				"PCa with PTEN loss demonstrates a strong correlation with known unfavorable histologic features, particularly IDC-P. This is the first study showing the association of PTEN loss with stromogenic PCa.";
		String sectionAnnotsBionlp = "T1	title 0 171	PTEN loss in prostatic adenocarcinoma correlates with specific adverse histologic features (intraductal carcinoma, cribriform Gleason pattern 4 and stromogenic carcinoma).\n" + 
				"T2	abstract 173 2210	The loss of PTEN tumor suppressor gene is one of the most common somatic genetic aberrations in prostate cancer (PCa) and is frequently associated with high-risk disease. Deletion or mutation of at least one PTEN allele has been reported to occur in 20% to 40% of localized PCa and up to 60% of metastases. The goal of this study was to determine if somatic alteration detected by PTEN immunohistochemical loss of expression is associated with specific histologic features. Two hundred sixty prostate core needle biopsies with PCa were assessed for PTEN loss using an analytically validated immunohistochemical assay. Blinded to PTEN status, each tumor was assessed for the Grade Group (GG) and the presence or absence of nine epithelial features. Presence of stromogenic PCa was also assessed and defined as grade 3 reactive tumor stroma as previously described: the presence of carcinoma associated stromal response with epithelial to stroma ratio of greater than 50% reactive stroma. Eight-eight (34%) cases exhibited PTEN loss while 172 (66%) had intact PTEN. PTEN loss was significantly (P < 0.05) associated with increasing GG, poorly formed glands (74% of total cases with loss vs 49% of intact), and three well-validated unfavorable pathological features: intraductal carcinoma of the prostate (IDC-P) (69% of total cases with loss vs 12% of intact), cribriform Gleason pattern 4 (38% of total cases with loss vs 10% of intact) and stromogenic PCa (23% of total cases with loss vs 6% of intact). IDC-P had the highest relative risk (4.993, 95% confidence interval, 3.451-7.223, P < 0.001) for PTEN loss. At least one of these three unfavorable pathological features were present in 67% of PCa exhibiting PTEN loss, while only 11% of PCa exhibited PTEN loss when none of these three unfavorable pathological features were present. PCa with PTEN loss demonstrates a strong correlation with known unfavorable histologic features, particularly IDC-P. This is the first study showing the association of PTEN loss with stromogenic PCa.\n";
		String conceptAnnotsBionlp = "T1	MONDO:0004970 23 37	adenocarcinoma\n" + 
				"T2	MONDO:0005023 92 113	intraductal carcinoma\n" + 
				"T3	HP:0030731 104 113	carcinoma\n" + 
				"T4	MONDO:0004993 160 169	carcinoma\n" + 
				"T5	HP:0030731 160 169	carcinoma\n" + 
				"T6	PR:000028746 185 189	PTEN\n" + 
				"T7	MONDO:0005070 190 195	tumor\n" + 
				"T8	HP:0002664 190 195	tumor\n" + 
				"T9	SO:0000704 207 211	gene\n" + 
				"T10	UBERON:0002367 269 277	prostate\n" + 
				"T11	HP:0012125 269 284	prostate cancer\n" + 
				"T12	MONDO:0008315 269 284	prostate cancer\n" + 
				"T13	DRUGBANK:DB03088 286 289	PCa\n" + 
				"T14	SO:0000159 344 352	Deletion\n" + 
				"T15	SO:0001023 386 392	allele\n" + 
				"T16	DRUGBANK:DB03088 447 450	PCa\n" + 
				"T17	DRUGBANK:DB03088 700 703	PCa\n" + 
				"T18	MONDO:0005070 820 825	tumor\n" + 
				"T19	HP:0002664 820 825	tumor\n" + 
				"T20	DRUGBANK:DB03088 945 948	PCa\n" + 
				"T21	MONDO:0005070 999 1004	tumor\n" + 
				"T22	HP:0002664 999 1004	tumor\n" + 
				"T23	HP:0030731 1053 1062	carcinoma\n" + 
				"T24	MONDO:0004993 1053 1062	carcinoma\n" + 
				"T25	UBERON:0003891 1152 1158	stroma\n" + 
				"T26	MONDO:0005023 1437 1458	intraductal carcinoma\n" + 
				"T27	MONDO:0005159 1449 1474	carcinoma of the prostate\n" + 
				"T28	DRUGBANK:DB03088 1625 1628	PCa\n" + 
				"T29	DRUGBANK:DB03088 1870 1873	PCa\n" + 
				"T30	DRUGBANK:DB03088 1914 1917	PCa\n" + 
				"T31	DRUGBANK:DB03088 2011 2014	PCa\n" + 
				"T32	DRUGBANK:DB03088 2206 2209	PCa";
		String sentenceAnnotsBionlp = "T1	sentence 0 171	PTEN loss in prostatic adenocarcinoma correlates with specific adverse histologic features (intraductal carcinoma, cribriform Gleason pattern 4 and stromogenic carcinoma).\n" + 
				"T2	sentence 173 343	The loss of PTEN tumor suppressor gene is one of the most common somatic genetic aberrations in prostate cancer (PCa) and is frequently associated with high-risk disease.\n" + 
				"T3	sentence 344 479	Deletion or mutation of at least one PTEN allele has been reported to occur in 20% to 40% of localized PCa and up to 60% of metastases.\n" + 
				"T4	sentence 480 646	The goal of this study was to determine if somatic alteration detected by PTEN immunohistochemical loss of expression is associated with specific histologic features.\n" + 
				"T5	sentence 647 790	Two hundred sixty prostate core needle biopsies with PCa were assessed for PTEN loss using an analytically validated immunohistochemical assay.\n" + 
				"T6	sentence 791 920	Blinded to PTEN status, each tumor was assessed for the Grade Group (GG) and the presence or absence of nine epithelial features.\n" + 
				"T7	sentence 921 1159	Presence of stromogenic PCa was also assessed and defined as grade 3 reactive tumor stroma as previously described: the presence of carcinoma associated stromal response with epithelial to stroma ratio of greater than 50% reactive stroma.\n" + 
				"T8	sentence 1160 1236	Eight-eight (34%) cases exhibited PTEN loss while 172 (66%) had intact PTEN.\n" + 
				"T9	sentence 1237 1676	PTEN loss was significantly (P < 0.05) associated with increasing GG, poorly formed glands (74% of total cases with loss vs 49% of intact), and three well-validated unfavorable pathological features: intraductal carcinoma of the prostate (IDC-P) (69% of total cases with loss vs 12% of intact), cribriform Gleason pattern 4 (38% of total cases with loss vs 10% of intact) and stromogenic PCa (23% of total cases with loss vs 6% of intact).\n" + 
				"T10	sentence 1677 1758	IDC-P had the highest relative risk (4.993, 95% confidence interval, 3.451-7.223,\n" + 
				"T11	sentence 1759 1784	P < 0.001) for PTEN loss.\n" + 
				"T12	sentence 1785 2010	At least one of these three unfavorable pathological features were present in 67% of PCa exhibiting PTEN loss, while only 11% of PCa exhibited PTEN loss when none of these three unfavorable pathological features were present.\n" + 
				"T13	sentence 2011 2127	PCa with PTEN loss demonstrates a strong correlation with known unfavorable histologic features, particularly IDC-P.\n" + 
				"T14	sentence 2128 2210	This is the first study showing the association of PTEN loss with stromogenic PCa.";
		// @formatter:on

//		List<TextAnnotation> sentenceAnnotations = populateSentenceAnnotations();
//		List<TextAnnotation> sectionAnnots = Arrays
//				.asList(factory.createAnnotation(0, 165, documentText, "introduction"));

		DocumentCriteria textDc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria sentenceDc = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0");
		DocumentCriteria conceptAllDc = new DocumentCriteria(DocumentType.CONCEPT_ALL, DocumentFormat.BIONLP,
				PipelineKey.CONCEPT_POST_PROCESS, "0.1.0");
		DocumentCriteria sectionDc = new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");

		ProcessingStatus status = new ProcessingStatus(documentId);
		status.addCollection("PUBMED");
		status.addCollection("PUBMED_SUB_31");

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.SENTENCE_DONE);
		status.enableFlag(ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE);

		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();
		map.put(textDc, documentText);
		map.put(sentenceDc, sentenceAnnotsBionlp);
		map.put(conceptAllDc, conceptAnnotsBionlp);
		map.put(sectionDc, sectionAnnotsBionlp);

		// the word sentence appears in all of the sentences
		Set<String> keywords = new HashSet<String>(
				Arrays.asList("loss of function", "loss-of-function", "gain of function", "gain-of-function", "loss",
						"loses", "lose", "lost", "losing", "gain", "gains", "gained", "gaining"));
		Map<List<String>, String> suffixToPlaceholderMap = new HashMap<List<String>, String>();
		suffixToPlaceholderMap.put(Arrays.asList("PR"), "@GENE$");
		suffixToPlaceholderMap.put(Arrays.asList("MONDO", "HP"), "@DISEASE$");

		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, map);

		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.extractSentences(documentId, documentText,
				DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED, docTypeToContentMap, keywords,
				suffixToPlaceholderMap, DocumentType.CONCEPT_ALL, new HashMap<String, Set<String>>(),
				new HashSet<String>());

//		ExtractedSentence esXfirst = new ExtractedSentence(documentId, X_000001, "ConceptX1",
//				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, Y_000001, "conceptY1",
//				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, "sentence", sentence2, documentText,
//				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);
//		ExtractedSentence esYfirst = new ExtractedSentence(documentId, Y_000001, "conceptY1",
//				CollectionsUtil.createList(y1Sentence2Span), PLACEHOLDER_Y, X_000001, "ConceptX1",
//				CollectionsUtil.createList(x1Sentence2Span), PLACEHOLDER_X, "sentence", sentence2, documentText,
//				DOCUMENT_ZONE, DOCUMENT_PUBLICATION_TYPES, DOCUMENT_YEAR_PUBLISHED);

		
		for (ExtractedSentence sent : extractedSentences) {
			System.out.println(sent.getSentenceWithPlaceholders());
		}
		
		assertEquals("there should be two extracted sentences", 2, extractedSentences.size());
//		// b/c order is not guaranteed, we check for either case
//		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));
	}

}
