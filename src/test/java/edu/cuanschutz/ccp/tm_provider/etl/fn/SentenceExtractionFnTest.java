package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.values.KV;
import org.junit.Before;
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
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class SentenceExtractionFnTest {

	private static final String Y_000001 = "Y:000001";
	private static final String X_000001 = "X:000001";
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

	private static TextAnnotation x1Sentence1Annot = factory.createAnnotation(18, 27, "conceptX1", X_000001);
	private static TextAnnotation x2Sentence1Annot = factory.createAnnotation(32, 41, "conceptX2", "X:000002");
	private static TextAnnotation x1Sentence2Annot = factory.createAnnotation(43, 52, "ConceptX1", X_000001);
	private static TextAnnotation x1Sentence4Annot = factory.createAnnotation(135, 144, "ConceptX1", X_000001);
	private static TextAnnotation y1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

	// This is simulating the concept Y_000001 existing in both the X & Y ontologies, e.g. an extension class
	private static TextAnnotation x3ReallyY1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

	private TextAnnotation sentence1Annot = factory.createAnnotation(0, 42, sentence1, SENTENCE);
	private TextAnnotation sentence2Annot = factory.createAnnotation(43, 94, sentence2, SENTENCE);
	private TextAnnotation sentence3Annot = factory.createAnnotation(95, 134, sentence3, SENTENCE);
	private TextAnnotation sentence4Annot = factory.createAnnotation(135, 165, sentence4, SENTENCE);

	@Before
	public void setUp() {
		populateSentenceAnnotations();
		populateConceptAnnotations();
	}

	private void populateConceptAnnotations() {
		conceptXAnnots = new ArrayList<TextAnnotation>();
		conceptXAnnots.add(x1Sentence1Annot);
		conceptXAnnots.add(x2Sentence1Annot);
		conceptXAnnots.add(x1Sentence2Annot);
		conceptXAnnots.add(x1Sentence4Annot);

		conceptYAnnots = new ArrayList<TextAnnotation>();
		conceptYAnnots.add(y1Sentence2Annot);

	}

	private void populateSentenceAnnotations() {
		sentenceAnnotations = new ArrayList<TextAnnotation>();
		sentenceAnnotations.add(sentence1Annot);
		sentenceAnnotations.add(sentence2Annot);
		sentenceAnnotations.add(sentence3Annot);
		sentenceAnnotations.add(sentence4Annot);
	}

	@Test
	public void testBuildSentenceToConceptMap() {
		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentToConceptMap = SentenceExtractionFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);

		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> expectedSentToConceptMap = new HashMap<TextAnnotation, Map<String, Set<TextAnnotation>>>();
		Map<String, Set<TextAnnotation>> map1 = new HashMap<String, Set<TextAnnotation>>();
		map1.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(x1Sentence1Annot, x2Sentence1Annot));
		expectedSentToConceptMap.put(sentence1Annot, map1);

		Map<String, Set<TextAnnotation>> map2 = new HashMap<String, Set<TextAnnotation>>();
		map2.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(x1Sentence2Annot));
		map2.put(SentenceExtractionFn.Y_CONCEPTS, CollectionsUtil.createSet(y1Sentence2Annot));
		expectedSentToConceptMap.put(sentence2Annot, map2);

		Map<String, Set<TextAnnotation>> map4 = new HashMap<String, Set<TextAnnotation>>();
		map4.put(SentenceExtractionFn.X_CONCEPTS, CollectionsUtil.createSet(x1Sentence4Annot));
		expectedSentToConceptMap.put(sentence4Annot, map4);

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
		List<TextAnnotation> crfAnnots = new ArrayList<TextAnnotation>();
		crfAnnots.add(factory.createAnnotation(32, 41, "conceptX2", "X:000002"));
		crfAnnots.add(factory.createAnnotation(49, 55, "ConceptX1", X_000001));

		List<TextAnnotation> expectedAnnots = new ArrayList<TextAnnotation>();
		expectedAnnots.add(factory.createAnnotation(32, 41, "conceptX2", "X:000002"));
		expectedAnnots.add(factory.createAnnotation(43, 52, "ConceptX1", X_000001));

		List<TextAnnotation> filteredAnnots = PipelineMain.filterViaCrf(conceptXAnnots, crfAnnots);

		assertEquals(expectedAnnots, filteredAnnots);
	}

	@Test
	public void testCatalogExtractedSentences() {
		Set<String> keywords = CollectionsUtil.createSet("sentence");

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1", "[[43..52]]", Y_000001,
				"conceptY1", "[[84..93]]", "sentence", sentence2, documentText);
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
		Set<String> keywords = CollectionsUtil.createSet("sentence");

		// this is concept X but is part of the y ontology so it potentially creates a
		// situation where an ExtractedSentence contains two references to concept X1.
		conceptXAnnots.add(x3ReallyY1Sentence2Annot);

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1", "[[43..52]]", Y_000001,
				"conceptY1", "[[84..93]]", "sentence", sentence2, documentText);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	private Set<ExtractedSentence> extractSentences(Set<String> keywords) {
		Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionFn
				.buildSentenceToConceptMap(sentenceAnnotations, conceptXAnnots, conceptYAnnots);

		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.catalogExtractedSentences(keywords,
				documentText, documentId, sentenceToConceptMap);
		return extractedSentences;
	}

	@Test
	public void testCatalogExtractedSentencesNoKeyword() {
		Set<String> keywords = null;

		Set<ExtractedSentence> extractedSentences = extractSentences(keywords);

		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();
		ExtractedSentence es = new ExtractedSentence(documentId, X_000001, "ConceptX1", "[[43..52]]", Y_000001,
				"conceptY1", "[[84..93]]", null, sentence2, documentText);
		expectedExtractedSentences.add(es);

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testCatalogExtractedSentencesKeywordNotFound() {
		Set<String> keywords = CollectionsUtil.createSet("notfound");
		Set<ExtractedSentence> extractedSentences = extractSentences(keywords);

		// keyword id not found, so no sentences are extracted
		Set<ExtractedSentence> expectedExtractedSentences = new HashSet<ExtractedSentence>();

		assertEquals(expectedExtractedSentences, extractedSentences);

	}

	@Test
	public void testExtractSentences() throws IOException {

		DocumentCriteria textDc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria conceptChebiDc = new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP,
				PipelineKey.OGER, "0.1.0");
		DocumentCriteria crfChebiDc = new DocumentCriteria(DocumentType.CRF_CHEBI, DocumentFormat.BIONLP,
				PipelineKey.CRF, "0.1.0");
		DocumentCriteria conceptPrDc = new DocumentCriteria(DocumentType.CONCEPT_PR, DocumentFormat.BIONLP,
				PipelineKey.OGER, "0.1.0");
		DocumentCriteria crfPrDc = new DocumentCriteria(DocumentType.CRF_PR, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.1.0");
		DocumentCriteria sentenceDc = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0");
		Set<DocumentCriteria> requiredDocCriteria = CollectionsUtil.createSet(textDc, conceptChebiDc, crfChebiDc,
				conceptPrDc, crfPrDc, sentenceDc);

		ProcessingStatus status = new ProcessingStatus(documentId);
		status.addCollection("PUBMED");
		status.addCollection("PUBMED_SUB_0");

		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.SENTENCE_DONE);
		status.enableFlag(ProcessingStatusFlag.OGER_CHEBI_DONE);
		status.enableFlag(ProcessingStatusFlag.CRF_CHEBI_DONE);
		status.enableFlag(ProcessingStatusFlag.OGER_PR_DONE);
		status.enableFlag(ProcessingStatusFlag.CRF_PR_DONE);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		String sentenceBionlp = null;
		String conceptChebiBionlp = null;
		String crfChebiBionlp = null;
		String conceptPrBionlp = null;
		String crfPrBionlp = null;

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(sentenceAnnotations);
			writer.serialize(td, outputStream, encoding);
			sentenceBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(conceptXAnnots);
			writer.serialize(td, outputStream, encoding);
			conceptChebiBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(conceptYAnnots);
			writer.serialize(td, outputStream, encoding);
			conceptPrBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		// the CRF annots overlap with some but not all of the concept annots
		List<TextAnnotation> crfXAnnots = new ArrayList<TextAnnotation>();
		crfXAnnots.add(x2Sentence1Annot);
		crfXAnnots.add(x1Sentence2Annot);

		List<TextAnnotation> crfYAnnots = new ArrayList<TextAnnotation>();
		crfYAnnots.add(y1Sentence2Annot);

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(crfXAnnots);
			writer.serialize(td, outputStream, encoding);
			crfChebiBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			TextDocument td = new TextDocument(documentId, "Pubmed", documentText);
			td.addAnnotations(crfYAnnots);
			writer.serialize(td, outputStream, encoding);
			crfPrBionlp = outputStream.toString(encoding.getCharacterSetName());
		}

		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();
		map.put(textDc, documentText);
		map.put(sentenceDc, sentenceBionlp);
		map.put(conceptChebiDc, conceptChebiBionlp);
		map.put(crfChebiDc, crfChebiBionlp);
		map.put(conceptPrDc, conceptPrBionlp);
		map.put(crfPrDc, crfPrBionlp);

		KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = KV.of(status, map);

		// the word sentence appears in all of the sentences
		Set<String> keywords = CollectionsUtil.createSet("sentence");
		Set<ExtractedSentence> extractedSentences = SentenceExtractionFn.extractSentences(requiredDocCriteria,
				statusEntityToText, keywords);
		ExtractedSentence esXfirst = new ExtractedSentence(documentId, X_000001, "ConceptX1", "[[43..52]]", Y_000001,
				"conceptY1", "[[84..93]]", "sentence", sentence2, documentText);
		ExtractedSentence esYfirst = new ExtractedSentence(documentId, Y_000001, "conceptY1", "[[84..93]]", X_000001,
				"ConceptX1", "[[43..52]]", "sentence", sentence2, documentText);

		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// no keywords
		keywords = null;
		extractedSentences = SentenceExtractionFn.extractSentences(requiredDocCriteria, statusEntityToText, keywords);
		esXfirst = new ExtractedSentence(documentId, X_000001, "ConceptX1", "[[43..52]]", Y_000001, "conceptY1",
				"[[84..93]]", null, sentence2, documentText);
		esYfirst = new ExtractedSentence(documentId, Y_000001, "conceptY1", "[[84..93]]", X_000001, "ConceptX1",
				"[[43..52]]", null, sentence2, documentText);
		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// no keywords
		keywords = new HashSet<String>();
		extractedSentences = SentenceExtractionFn.extractSentences(requiredDocCriteria, statusEntityToText, keywords);
		assertEquals("there should be a single extracted sentence", 1, extractedSentences.size());
		// b/c order is not guaranteed, we check for either case
		assertTrue(extractedSentences.contains(esXfirst) || extractedSentences.contains(esYfirst));

		// keyword not found so no sentence extracted
		keywords = CollectionsUtil.createSet("notfound");
		extractedSentences = SentenceExtractionFn.extractSentences(requiredDocCriteria, statusEntityToText, keywords);
		assertEquals("there should be no extracted sentences", 0, extractedSentences.size());

	}

}
