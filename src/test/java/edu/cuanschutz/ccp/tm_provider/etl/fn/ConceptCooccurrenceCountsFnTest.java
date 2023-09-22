package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.CrfOrConcept;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.ConceptPair;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ConceptCooccurrenceCountsFnTest {

	private static final String Y_000001 = "CL:000001";
	private static final String X_000001 = "PR:000001";
	private static final String X_000002 = "PR:000002";

	private static final String Y_000000 = "CL:000000";
	private static final String X_000000 = "PR:000000";
	private static final String X_000003 = "PR:000003";

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
	private static TextAnnotation x2Sentence1Annot = factory.createAnnotation(32, 41, "conceptX2", X_000002);
	private static TextAnnotation x1Sentence2Annot = factory.createAnnotation(43, 52, "ConceptX1", X_000001);
	private static TextAnnotation x1Sentence4Annot = factory.createAnnotation(135, 144, "ConceptX1", X_000001);
	private static TextAnnotation y1Sentence2Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000001);

//	private static TextAnnotation x1Sentence1Ancestor0Annot = factory.createAnnotation(18, 27, "conceptX1", X_000000);
//	private static TextAnnotation x2Sentence1Ancestor1Annot = factory.createAnnotation(32, 41, "conceptX2", X_000001);
//	private static TextAnnotation x2Sentence1Ancestor0Annot = factory.createAnnotation(32, 41, "conceptX2", X_000000);
//	private static TextAnnotation x1Sentence2Ancestor0Annot = factory.createAnnotation(43, 52, "ConceptX1", X_000000);
//	private static TextAnnotation x1Sentence4Ancestor0Annot = factory.createAnnotation(135, 144, "ConceptX1", X_000000);
//	private static TextAnnotation y1Sentence2Ancestor0Annot = factory.createAnnotation(84, 93, "conceptY1", Y_000000);

	private static TextAnnotation sentence1Annot = factory.createAnnotation(0, 42, sentence1, SENTENCE);
	private static TextAnnotation sentence2Annot = factory.createAnnotation(43, 94, sentence2, SENTENCE);
	private static TextAnnotation sentence3Annot = factory.createAnnotation(95, 134, sentence3, SENTENCE);
	private static TextAnnotation sentence4Annot = factory.createAnnotation(135, 165, sentence4, SENTENCE);

	private static TextAnnotation titleSectionAnnot = factory.createAnnotation(0, 42, sentence1, "title");
	private static TextAnnotation abstractSectionAnnot = factory.createAnnotation(43, 165, sentence1, "abstract");
	private static TextAnnotation documentAnnot = factory.createAnnotation(0, 165, sentence1, "document");

//	private static List<TextAnnotation> conceptAnnots = Arrays.asList(y1Sentence2Annot, x1Sentence1Annot,
//			x2Sentence1Annot, x1Sentence2Annot, x1Sentence4Annot);

	private static List<TextAnnotation> conceptXAnnots = Arrays.asList(x1Sentence1Annot, x2Sentence1Annot,
			x1Sentence2Annot, x1Sentence4Annot);
	private static List<TextAnnotation> conceptYAnnots = Arrays.asList(y1Sentence2Annot);
	private static List<TextAnnotation> crfXAnnots = Arrays.asList(x2Sentence1Annot, x1Sentence2Annot);
	private static List<TextAnnotation> crfYAnnots = Arrays.asList(y1Sentence2Annot);

	private static List<TextAnnotation> allConceptsPostCrfFilter = Arrays.asList(x2Sentence1Annot, x1Sentence2Annot,
			y1Sentence2Annot);

//	private static List<TextAnnotation> conceptAncestorAnnots = Arrays.asList(y1Sentence2Ancestor0Annot,
//			x1Sentence1Ancestor0Annot, x2Sentence1Ancestor0Annot, x2Sentence1Ancestor1Annot, x1Sentence2Ancestor0Annot,
//			x1Sentence4Ancestor0Annot);
	private static List<TextAnnotation> sentenceAnnots = Arrays.asList(sentence1Annot, sentence2Annot, sentence3Annot,
			sentence4Annot);
	private static List<TextAnnotation> sectionAnnots = Arrays.asList(titleSectionAnnot, abstractSectionAnnot);

	private static Map<String, Set<String>> ancestorMap;

	private static String sentenceAnnotationBionlp;
	private static String sectionAnnotationBionlp;
	private static String conceptXAnnotationBionlp;
	private static String crfXAnnotationBionlp;
	private static String conceptYAnnotationBionlp;
//	private static String crfYAnnotationBionlp;
	private static String conceptAllAnnotationBionlp;

	// this map contains all CONCEPT and CRF annotations plus text, sentences, etc.
	private static Map<DocumentCriteria, String> docCriteriaToContentMapPreCrfFiltering;

	// this map contains a single aggregated CONCEPT_ALL annotation string, plus
	// text, sentences, etc.
	private static Map<DocumentCriteria, String> docCriteriaToContentMapPostCrfFiltering;

	@BeforeClass
	public static void setUp() throws IOException {
		ancestorMap = new HashMap<String, Set<String>>();
		ancestorMap.put(X_000001, CollectionsUtil.createSet(X_000000));
		ancestorMap.put(X_000002, CollectionsUtil.createSet(X_000000, X_000001));
		ancestorMap.put(X_000003, CollectionsUtil.createSet(X_000000, X_000001, X_000002));
		ancestorMap.put(Y_000001, CollectionsUtil.createSet(Y_000000, "XYZ:some_parent_class"));

		TextDocument sentenceDoc = new TextDocument(documentId, "PubMed", documentText);
		sentenceDoc.addAnnotations(sentenceAnnots);

		TextDocument sectionDoc = new TextDocument(documentId, "PubMed", documentText);
		sectionDoc.addAnnotations(sectionAnnots);

		TextDocument conceptDocX = new TextDocument(documentId, "PubMed", documentText);
		conceptDocX.addAnnotations(conceptXAnnots);

		TextDocument crfDocX = new TextDocument(documentId, "PubMed", documentText);
		crfDocX.addAnnotations(crfXAnnots);

		TextDocument conceptDocY = new TextDocument(documentId, "PubMed", documentText);
		conceptDocY.addAnnotations(conceptYAnnots);

		TextDocument crfDocY = new TextDocument(documentId, "PubMed", documentText);
		crfDocY.addAnnotations(crfYAnnots);

		TextDocument allConceptsDoc = new TextDocument(documentId, "PubMed", documentText);
		allConceptsDoc.addAnnotations(allConceptsPostCrfFilter);

		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			writer.serialize(sentenceDoc, outStream, encoding);
			sentenceAnnotationBionlp = outStream.toString();
		}
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			writer.serialize(sectionDoc, outStream, encoding);
			sectionAnnotationBionlp = outStream.toString();
		}

		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			writer.serialize(conceptDocX, outStream, encoding);
			conceptXAnnotationBionlp = outStream.toString();
		}
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			writer.serialize(conceptDocY, outStream, encoding);
			conceptYAnnotationBionlp = outStream.toString();
		}
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			crfDocX.addAnnotations(crfDocY.getAnnotations());
			writer.serialize(crfDocX, outStream, encoding);
			crfXAnnotationBionlp = outStream.toString();
		}
//		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
//			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
//			writer.serialize(crfDocY, outStream, encoding);
//			crfYAnnotationBionlp = outStream.toString();
//		}
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			writer.serialize(allConceptsDoc, outStream, encoding);
			conceptAllAnnotationBionlp = outStream.toString();
		}

		docCriteriaToContentMapPreCrfFiltering = new HashMap<DocumentCriteria, String>();

		docCriteriaToContentMapPreCrfFiltering.put(new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0"), sentenceAnnotationBionlp);
		docCriteriaToContentMapPreCrfFiltering.put(new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0"), sectionAnnotationBionlp);
		docCriteriaToContentMapPreCrfFiltering.put(
				new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0"),
				documentText);
		docCriteriaToContentMapPreCrfFiltering.put(
				new DocumentCriteria(DocumentType.CONCEPT_CS, DocumentFormat.BIONLP, PipelineKey.OGER, "0.1.0"),
				conceptXAnnotationBionlp);
		docCriteriaToContentMapPreCrfFiltering.put(
				new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF, "0.1.0"),
				crfXAnnotationBionlp);
		docCriteriaToContentMapPreCrfFiltering.put(
				new DocumentCriteria(DocumentType.CONCEPT_CIMIN, DocumentFormat.BIONLP, PipelineKey.OGER, "0.1.0"),
				conceptYAnnotationBionlp);
//		docCriteriaToContentMapPreCrfFiltering.put(
//				new DocumentCriteria(DocumentType.CRF_CL, DocumentFormat.BIONLP, PipelineKey.CRF, "0.1.0"),
//				crfYAnnotationBionlp);

		docCriteriaToContentMapPostCrfFiltering = new HashMap<DocumentCriteria, String>();
		docCriteriaToContentMapPostCrfFiltering.put(new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PipelineKey.SENTENCE_SEGMENTATION, "0.1.0"), sentenceAnnotationBionlp);
		docCriteriaToContentMapPostCrfFiltering.put(new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0"), sectionAnnotationBionlp);
		docCriteriaToContentMapPostCrfFiltering.put(
				new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0"),
				documentText);
		docCriteriaToContentMapPostCrfFiltering.put(new DocumentCriteria(DocumentType.CONCEPT_ALL,
				DocumentFormat.BIONLP, PipelineKey.CONCEPT_POST_PROCESS, "0.1.0"), conceptAllAnnotationBionlp);

//		DocumentCriteria xConceptCriteria = new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP,
//				PipelineKey.OGER, "0.1.0");
//		DocumentCriteria xCrfCriteria = new DocumentCriteria(DocumentType.CRF_CHEBI, DocumentFormat.BIONLP,
//				PipelineKey.CRF, "0.1.0");
//		DocumentCriteria yConceptCriteria = new DocumentCriteria(DocumentType.CONCEPT_CL, DocumentFormat.BIONLP,
//				PipelineKey.OGER, "0.1.0");
//		DocumentCriteria yCrfCriteria = new DocumentCriteria(DocumentType.CRF_CL, DocumentFormat.BIONLP,
//				PipelineKey.CRF, "0.1.0");
//
//		Map<DocumentCriteria, String> inputDocumentMap = new HashMap<DocumentCriteria, String>();
//		inputDocumentMap.put(xConceptCriteria, conceptXAnnotationBionlp);
//		inputDocumentMap.put(xCrfCriteria, crfXAnnotationBionlp);
//		inputDocumentMap.put(yConceptCriteria, conceptYAnnotationBionlp);
//		inputDocumentMap.put(yCrfCriteria, crfYAnnotationBionlp);

	}

	@Test
	public void testCountConceptsSentenceLevel() throws IOException {

		CooccurLevel level = CooccurLevel.SENTENCE;
		Map<String, Set<String>> textIdToSingletonConceptIdsMap = ConceptCooccurrenceCountsFn.countConcepts(documentId,
				docCriteriaToContentMapPostCrfFiltering, level, DocumentType.CONCEPT_ALL);

		String sentenceId1 = ConceptCooccurrenceCountsFn.computeUniqueTextIdentifier(documentId, level, sentence1Annot);
		String sentenceId2 = ConceptCooccurrenceCountsFn.computeUniqueTextIdentifier(documentId, level, sentence2Annot);

		Map<String, Set<String>> expectedTextIdToSingletonConceptIdsMap = new HashMap<String, Set<String>>();
		expectedTextIdToSingletonConceptIdsMap.put(sentenceId1, CollectionsUtil.createSet(X_000002));
		expectedTextIdToSingletonConceptIdsMap.put(sentenceId2, CollectionsUtil.createSet(X_000001, Y_000001));

		assertEquals(expectedTextIdToSingletonConceptIdsMap.size(), textIdToSingletonConceptIdsMap.size());
		assertEquals(expectedTextIdToSingletonConceptIdsMap, textIdToSingletonConceptIdsMap);

	}

	@Test
	public void testCountConceptsDocumentLevel() throws IOException {

		CooccurLevel level = CooccurLevel.DOCUMENT;
		Map<String, Set<String>> textIdToSingletonConceptIdsMap = ConceptCooccurrenceCountsFn.countConcepts(documentId,
				docCriteriaToContentMapPostCrfFiltering, level, DocumentType.CONCEPT_ALL);

		String docId = ConceptCooccurrenceCountsFn.computeUniqueTextIdentifier(documentId, level, documentAnnot);

		Map<String, Set<String>> expectedTextIdToSingletonConceptIdsMap = new HashMap<String, Set<String>>();
		expectedTextIdToSingletonConceptIdsMap.put(docId, CollectionsUtil.createSet(X_000002, X_000001, Y_000001));

		assertEquals(expectedTextIdToSingletonConceptIdsMap.size(), textIdToSingletonConceptIdsMap.size());
		assertEquals(expectedTextIdToSingletonConceptIdsMap, textIdToSingletonConceptIdsMap);

	}

	// TODO: move test to PipelineMainTest as this method was moved into
	// PipelineMain
	@Test
	public void testSpliceUniqueValues() {
		Map<String, Collection<String>> map = new HashMap<String, Collection<String>>();
		map.put("1", Arrays.asList("1", "2", "3"));
		map.put("2", Arrays.asList("4", "5", "6"));
		map.put("3", Arrays.asList("4", "5", "7"));

		Set<String> expectedList = CollectionsUtil.createSet("1", "2", "3", "4", "5", "6", "7");
		Set<String> outputList = PipelineMain.spliceValues(map);
		assertEquals(expectedList, outputList);
	}

	@Test
	public void testMatchConceptsToLevelAnnots() {

		List<TextAnnotation> conceptAnnots = Arrays.asList(y1Sentence2Annot, x1Sentence1Annot, x2Sentence1Annot,
				x1Sentence2Annot, x1Sentence4Annot);

		List<TextAnnotation> levelAnnots = Arrays.asList(sentence1Annot, sentence2Annot, sentence3Annot,
				sentence4Annot);

		Map<TextAnnotation, Set<TextAnnotation>> outputMap = ConceptCooccurrenceCountsFn
				.matchConceptsToLevelAnnots(levelAnnots, conceptAnnots);

		Map<TextAnnotation, Set<TextAnnotation>> expectedMap = new HashMap<TextAnnotation, Set<TextAnnotation>>();
		expectedMap.put(sentence1Annot, CollectionsUtil.createSet(x1Sentence1Annot, x2Sentence1Annot));
		expectedMap.put(sentence2Annot, CollectionsUtil.createSet(y1Sentence2Annot, x1Sentence2Annot));
		expectedMap.put(sentence4Annot, CollectionsUtil.createSet(x1Sentence4Annot));

		assertEquals(expectedMap, outputMap);
	}

	// TODO: move test to PipelineMainTest as this method was moved into
	// PipelineMain
	@Test
	public void testGetConceptAnnotations() throws IOException {
//		String conceptX = "X";
//		String conceptY = "Y";
//
//		Map<String, Map<CrfOrConcept, String>> conceptTypeToContentMap = new HashMap<String, Map<CrfOrConcept, String>>();
//		Map<CrfOrConcept, String> innerMapX = new HashMap<CrfOrConcept, String>();
//		innerMapX.put(CrfOrConcept.CONCEPT, conceptXAnnotationBionlp);
//		innerMapX.put(CrfOrConcept.CRF, crfXAnnotationBionlp);
//		conceptTypeToContentMap.put(conceptX, innerMapX);
//
//		Map<CrfOrConcept, String> innerMapY = new HashMap<CrfOrConcept, String>();
//		innerMapY.put(CrfOrConcept.CONCEPT, conceptYAnnotationBionlp);
//		innerMapY.put(CrfOrConcept.CRF, crfYAnnotationBionlp);
//		conceptTypeToContentMap.put(conceptY, innerMapY);

		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docCriteriaToContentMapPreCrfFiltering);

		// remove theme id slot from annotations to match expected
		for (Entry<DocumentType, Collection<TextAnnotation>> entry : docTypeToContentMap.entrySet()) {
			for (TextAnnotation annot : entry.getValue()) {
				annot.getClassMention().setPrimitiveSlotMentions(Collections.emptyList());
			}
		}

		Map<DocumentType, Set<TextAnnotation>> outputAnnotMap = PipelineMain
				.filterConceptAnnotations(docTypeToContentMap, FilterFlag.BY_CRF);

		Map<DocumentType, Set<TextAnnotation>> expectedAnnotMap = new HashMap<DocumentType, Set<TextAnnotation>>();
		expectedAnnotMap.put(DocumentType.CONCEPT_PR,
				new HashSet<TextAnnotation>(Arrays.asList(x2Sentence1Annot, x1Sentence2Annot)));
		expectedAnnotMap.put(DocumentType.CONCEPT_CL, new HashSet<TextAnnotation>(Arrays.asList(y1Sentence2Annot)));

		assertEquals(expectedAnnotMap.size(), outputAnnotMap.size());
		assertEquals(expectedAnnotMap, outputAnnotMap);
	}

	@Test
	public void testGetLevelAnnotationsDocument() throws IOException {

		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docCriteriaToContentMapPostCrfFiltering);

		List<TextAnnotation> outputList = ConceptCooccurrenceCountsFn.getLevelAnnotations(documentId,
				CooccurLevel.DOCUMENT, documentText, docTypeToContentMap);

		List<TextAnnotation> expectedList = Arrays.asList(documentAnnot);

		assertEquals(expectedList, outputList);
	}

	@Test
	public void testGetLevelAnnotationsSentences() throws IOException {
		Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
				.getDocTypeToContentMap(documentId, docCriteriaToContentMapPostCrfFiltering);

		List<TextAnnotation> outputList = ConceptCooccurrenceCountsFn.getLevelAnnotations(documentId,
				CooccurLevel.SENTENCE, documentText, docTypeToContentMap);

		List<TextAnnotation> expectedList = new ArrayList<TextAnnotation>(sentenceAnnots);

		Collections.sort(outputList, TextAnnotation.BY_SPAN());
		Collections.sort(expectedList, TextAnnotation.BY_SPAN());

		// remove theme id slot from annotations to match expected
		for (TextAnnotation annot : outputList) {
			annot.getClassMention().setPrimitiveSlotMentions(Collections.emptyList());
		}

		assertEquals(expectedList.size(), outputList.size());
		assertEquals(expectedList, outputList);
	}

	@Test
	public void testAddToMap() {
		Map<String, Map<CrfOrConcept, String>> map = new HashMap<String, Map<CrfOrConcept, String>>();
		String doc1 = "doc1";
		String type1 = "CHEBI";
		ConceptCooccurrenceCountsFn.addToMap(map, doc1, type1, CrfOrConcept.CRF);

		Map<String, Map<CrfOrConcept, String>> expectedMap = new HashMap<String, Map<CrfOrConcept, String>>();
		Map<CrfOrConcept, String> innerMap1 = new HashMap<CrfOrConcept, String>();
		innerMap1.put(CrfOrConcept.CRF, doc1);
		expectedMap.put(type1, innerMap1);

		assertEquals(expectedMap, map);

		String doc2 = "doc2";
		String type2 = "CL";
		ConceptCooccurrenceCountsFn.addToMap(map, doc2, type2, CrfOrConcept.CONCEPT);

		Map<CrfOrConcept, String> innerMap2 = new HashMap<CrfOrConcept, String>();
		innerMap2.put(CrfOrConcept.CONCEPT, doc2);
		expectedMap.put(type2, innerMap2);

		assertEquals(expectedMap, map);

		String doc3 = "doc3";
		ConceptCooccurrenceCountsFn.addToMap(map, doc3, type2, CrfOrConcept.CRF);
		innerMap2.put(CrfOrConcept.CRF, doc3);

		assertEquals(expectedMap, map);

	}

	@Test
	public void testConceptPairToReproducibleKey() {
		String id1 = "CL:0000001";
		String id2 = "CHEBI:12345";
		ConceptPair cp = new ConceptPair(id1, id2);

		String key = cp.toReproducibleKey();
		String expectedKey = id2 + "|" + id1;
		assertEquals(expectedKey, key);
	}

	@Test
	public void testConceptPairEquals() {
		ConceptPair cp1 = new ConceptPair("PR:0001", "CL:0002");
		ConceptPair cp2 = new ConceptPair("CL:0002", "PR:0001");

		assertEquals(cp1.hashCode(), cp2.hashCode());
		assertEquals(cp1, cp2);

	}

}
