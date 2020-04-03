package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Layer;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Relation;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.TableKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class BigQueryAnnotationSerializerTest {

	/* @formatter:off */
	// 1 1 1 1 1 1 1 1 1 1 2 2 2
	// 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2
	// 01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
	private static final String DOCUMENT_TEXT = "This is a sentence with a concept. Here is another sentence with a different concept.  This sentence is at the beginning of a new paragraph. This paragraph has two sentences, with the second sentence containing a concept.";
	private TextAnnotation sectionAnnot_1;
	/* @formatter:on */
	private TextAnnotation sectionAnnot_2;
	private TextAnnotation paragraphAnnot_1;
	private TextAnnotation paragraphAnnot_2;
	private TextAnnotation sentenceAnnot_1;
	private TextAnnotation sentenceAnnot_2;
	private TextAnnotation sentenceAnnot_3;
	private TextAnnotation sentenceAnnot_4;
	private TextAnnotation conceptAnnot_1;
	private TextAnnotation conceptAnnot_2;
	private TextAnnotation conceptAnnot_3;
	private TextDocument td;
	private Map<Span, Set<String>> sectionSpanToIdMap;
	private Map<Span, Set<String>> paragraphSpanToIdMap;
	private Map<Span, Set<String>> sentenceSpanToIdMap;
	private Map<Span, Set<String>> conceptSpanToIdMap;
	private Map<String, Set<Relation>> annotationIdToRelationMap;
	private String docId;
	private String sectionText_1;
	private String sectionText_2;
	private String paragraphText_1;
	private String paragraphText_2;
	private String sentenceText_1;
	private String sentenceText_3;
	private String sentenceText_2;
	private String sentenceText_4;
	private String sectionId_1;
	private String sectionId_2;
	private String paragraphId_1;
	private String paragraphId_2;
	private String sentenceId_1;
	private String sentenceId_3;
	private String sentenceId_2;
	private String conceptId_1;
	private String sentenceId_4;
	private String conceptId_2;
	private String conceptId_3;
	private TextAnnotationFactory annotFactory;

	@Before
	public void setUp() {
		docId = "PMC12345";
		annotFactory = TextAnnotationFactory.createFactoryWithDefaults(docId);

		sectionText_1 = "This is a sentence with a concept. Here is another sentence with a different concept.  This sentence is at the beginning of a new paragraph. This paragraph has two sentences, with the second sentence containing a concept.";
		sectionAnnot_1 = annotFactory.createAnnotation(0, 221, sectionText_1, "INTRO", "bioc");
		sectionText_2 = "This is a sentence with a concept. Here is another sentence with a different concept.";
		sectionAnnot_2 = annotFactory.createAnnotation(0, 85, sectionText_2, "RESULTS", "bioc");

		paragraphText_1 = "This is a sentence with a concept. Here is another sentence with a different concept.";
		paragraphText_2 = "This sentence is at the beginning of a new paragraph. This paragraph has two sentences, with the second sentence containing a concept.";
		paragraphAnnot_1 = annotFactory.createAnnotation(0, 85, paragraphText_1, "paragraph", "bioc");
		paragraphAnnot_2 = annotFactory.createAnnotation(87, 221, paragraphText_2, "paragraph", "bioc");

		sentenceText_1 = "This is a sentence with a concept.";
		sentenceText_2 = "Here is another sentence with a different concept.";
		sentenceText_3 = "This sentence is at the beginning of a new paragraph.";
		sentenceText_4 = "This paragraph has two sentences, with the second sentence containing a concept.";
		sentenceAnnot_1 = annotFactory.createAnnotation(0, 34, sentenceText_1, "sentence", "turku");
		sentenceAnnot_2 = annotFactory.createAnnotation(35, 85, sentenceText_2, "sentence", "turku");
		sentenceAnnot_3 = annotFactory.createAnnotation(87, 140, sentenceText_3, "sentence", "turku");
		sentenceAnnot_4 = annotFactory.createAnnotation(141, 221, sentenceText_4, "sentence", "turku");

		conceptAnnot_1 = annotFactory.createAnnotation(26, 33, "concept", "CL:0000000", "oger");
		conceptAnnot_2 = annotFactory.createAnnotation(67, 84, "different concept", "HP:1234567", "oger");
		conceptAnnot_3 = annotFactory.createAnnotation(213, 220, "concept", "CL:0000000", "oger");

		td = new TextDocument(docId, "PMC", DOCUMENT_TEXT);
		td.addAnnotation(sectionAnnot_1);
		td.addAnnotation(sectionAnnot_2);
		td.addAnnotation(paragraphAnnot_1);
		td.addAnnotation(paragraphAnnot_2);
		td.addAnnotation(sentenceAnnot_1);
		td.addAnnotation(sentenceAnnot_2);
		td.addAnnotation(sentenceAnnot_3);
		td.addAnnotation(sentenceAnnot_4);
		td.addAnnotation(conceptAnnot_1);
		td.addAnnotation(conceptAnnot_2);
		td.addAnnotation(conceptAnnot_3);

		sectionId_1 = BigQueryUtil.getAnnotationIdentifier(sectionAnnot_1, Layer.SECTION);
		sectionId_2 = BigQueryUtil.getAnnotationIdentifier(sectionAnnot_2, Layer.SECTION);
		paragraphId_1 = BigQueryUtil.getAnnotationIdentifier(paragraphAnnot_1, Layer.PARAGRAPH);
		paragraphId_2 = BigQueryUtil.getAnnotationIdentifier(paragraphAnnot_2, Layer.PARAGRAPH);
		sentenceId_1 = BigQueryUtil.getAnnotationIdentifier(sentenceAnnot_1, Layer.SENTENCE);
		sentenceId_2 = BigQueryUtil.getAnnotationIdentifier(sentenceAnnot_2, Layer.SENTENCE);
		sentenceId_3 = BigQueryUtil.getAnnotationIdentifier(sentenceAnnot_3, Layer.SENTENCE);
		sentenceId_4 = BigQueryUtil.getAnnotationIdentifier(sentenceAnnot_4, Layer.SENTENCE);
		conceptId_1 = BigQueryUtil.getAnnotationIdentifier(conceptAnnot_1, Layer.CONCEPT);
		conceptId_2 = BigQueryUtil.getAnnotationIdentifier(conceptAnnot_2, Layer.CONCEPT);
		conceptId_3 = BigQueryUtil.getAnnotationIdentifier(conceptAnnot_3, Layer.CONCEPT);

		sectionSpanToIdMap = new HashMap<Span, Set<String>>();
		paragraphSpanToIdMap = new HashMap<Span, Set<String>>();
		sentenceSpanToIdMap = new HashMap<Span, Set<String>>();
		conceptSpanToIdMap = new HashMap<Span, Set<String>>();
		annotationIdToRelationMap = new HashMap<String, Set<Relation>>();

		BigQueryLoadBuilder.extractDocumentZoneAnnotations(td, sectionSpanToIdMap, paragraphSpanToIdMap,
				sentenceSpanToIdMap, conceptSpanToIdMap, annotationIdToRelationMap);

	}

	@Test
	public void testExtractDocumentZoneAnnotations() {

		Map<Span, Set<String>> expectedSectionSpanToIdMap = new HashMap<Span, Set<String>>();
		expectedSectionSpanToIdMap.put(sectionAnnot_1.getAggregateSpan(), CollectionsUtil.createSet(sectionId_1));
		expectedSectionSpanToIdMap.put(sectionAnnot_2.getAggregateSpan(), CollectionsUtil.createSet(sectionId_2));

		Map<Span, Set<String>> expectedParagraphSpanToIdMap = new HashMap<Span, Set<String>>();
		expectedParagraphSpanToIdMap.put(paragraphAnnot_1.getAggregateSpan(), CollectionsUtil.createSet(paragraphId_1));
		expectedParagraphSpanToIdMap.put(paragraphAnnot_2.getAggregateSpan(), CollectionsUtil.createSet(paragraphId_2));

		Map<Span, Set<String>> expectedSentenceSpanToIdMap = new HashMap<Span, Set<String>>();
		expectedSentenceSpanToIdMap.put(sentenceAnnot_1.getAggregateSpan(), CollectionsUtil.createSet(sentenceId_1));
		expectedSentenceSpanToIdMap.put(sentenceAnnot_2.getAggregateSpan(), CollectionsUtil.createSet(sentenceId_2));
		expectedSentenceSpanToIdMap.put(sentenceAnnot_3.getAggregateSpan(), CollectionsUtil.createSet(sentenceId_3));
		expectedSentenceSpanToIdMap.put(sentenceAnnot_4.getAggregateSpan(), CollectionsUtil.createSet(sentenceId_4));

		Map<Span, Set<String>> expectedConceptSpanToIdMap = new HashMap<Span, Set<String>>();
		expectedConceptSpanToIdMap.put(conceptAnnot_1.getAggregateSpan(), CollectionsUtil.createSet(conceptId_1));
		expectedConceptSpanToIdMap.put(conceptAnnot_2.getAggregateSpan(), CollectionsUtil.createSet(conceptId_2));
		expectedConceptSpanToIdMap.put(conceptAnnot_3.getAggregateSpan(), CollectionsUtil.createSet(conceptId_3));

		assertEquals(expectedSectionSpanToIdMap, sectionSpanToIdMap);
		assertEquals(expectedParagraphSpanToIdMap, paragraphSpanToIdMap);
		assertEquals(expectedSentenceSpanToIdMap, sentenceSpanToIdMap);
		assertEquals(expectedConceptSpanToIdMap, conceptSpanToIdMap);

		System.out.println("Section 1 ID: " + sectionId_1);
		System.out.println("Section 2 ID: " + sectionId_2);
		System.out.println("Paragraph 1 ID: " + paragraphId_1);
		System.out.println("Paragraph 2 ID: " + paragraphId_2);
		System.out.println("Sentence 1 ID: " + sentenceId_1);
		System.out.println("Sentence 2 ID: " + sentenceId_2);
		System.out.println("Sentence 3 ID: " + sentenceId_3);
		System.out.println("Sentence 4 ID: " + sentenceId_4);
		System.out.println("Concept 1 ID: " + conceptId_1);
		System.out.println("Concept 2 ID: " + conceptId_2);
		System.out.println("Concept 3 ID: " + conceptId_3);

	}

	
	@Test
	public void test () {
		String value = "asdf\n\n\n \n\n\n";
		while (StringUtil.endsWithRegex(value, "\\s")) {
			value = value.substring(0, value.length()-1);
		}
		assertEquals("asdf", value);
	}
	
	
	@Test
	public void testSectionAnnotSerialization() {

//		Map<String, Set<String>> typeToParentTypeMap = new HashMap<String, Set<String>>();
//		typeToParentTypeMap.put("http://purl.obolibrary.org/obo/CL_0000000", CollectionsUtil
//				.createSet("http://purl.obolibrary.org/obo/CL_0000001", "http://purl.obolibrary.org/obo/CL_0000002"));

//		Calendar publicationDate = Calendar.getInstance();
//		publicationDate.set(Calendar.YEAR, 2018);
//		publicationDate.set(Calendar.MONTH, 0);
//		publicationDate.set(Calendar.DAY_OF_MONTH, 1);
		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedSection1 = serializer.toString(sectionAnnot_1, DOCUMENT_TEXT);

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(sectionId_1 + "\tbioc\t" + docId + "\t"
				+ Layer.SECTION.name() + "\tINTRO\t" + sectionAnnot_1.getAggregateSpan().getSpanStart() + "\t"
				+ sectionAnnot_1.getAggregateSpan().getSpanEnd() + "\t\"" + sectionText_1 + "\"");

		assertEquals(expectedAnnotationLine, serializedSection1.get(TableKey.ANNOTATION));

	}
	
	@Test
	public void testSectionAnnotSerialization_sectionInSection() {

//		Map<String, Set<String>> typeToParentTypeMap = new HashMap<String, Set<String>>();
//		typeToParentTypeMap.put("http://purl.obolibrary.org/obo/CL_0000000", CollectionsUtil
//				.createSet("http://purl.obolibrary.org/obo/CL_0000001", "http://purl.obolibrary.org/obo/CL_0000002"));

//		Calendar publicationDate = Calendar.getInstance();
//		publicationDate.set(Calendar.YEAR, 2018);
//		publicationDate.set(Calendar.MONTH, 0);
//		publicationDate.set(Calendar.DAY_OF_MONTH, 1);
		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedSection2 = serializer.toString(sectionAnnot_2, DOCUMENT_TEXT);
		assertEquals(2, serializedSection2.size());

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(sectionId_2 + "\tbioc\t" + docId + "\t"
				+ Layer.SECTION.name() + "\tRESULTS\t" + sectionAnnot_2.getAggregateSpan().getSpanStart() + "\t"
				+ sectionAnnot_2.getAggregateSpan().getSpanEnd() + "\t\"" + sectionText_2 + "\"");

		Set<String> expectedInSectionLine = CollectionsUtil.createSet(sectionId_2 + "\t" + sectionId_1);
		assertEquals(expectedAnnotationLine, serializedSection2.get(TableKey.ANNOTATION));
		assertEquals(expectedInSectionLine, serializedSection2.get(TableKey.IN_SECTION));

	}
//
//		/* test serialization of section 2 annotation */
//		String expectedSerializedSection2Str = sectionId_2 + "\t" + sectionAnnot_2.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_1
//				+ "\tnull\tnull\tnull\tsection\tsection\ttrue\t0\t85\t\"" + sectionText_2 + "\"\tnull\tnull\tnull";
//		String serializedSection2Str = serializer.toString(sectionAnnot_2, DOCUMENT_TEXT).get(0);
//		assertEquals(expectedSerializedSection2Str, serializedSection2Str);
//

	@Test
	public void testParagraphAnnotSerialization() {

//		/* test serialization of paragraph 1 annotation */
//		String expectedSerializedParagraph1Str_a = paragraphId_1 + "\t" + paragraphAnnot_1.getAnnotator().getName()
//				+ "\t" + expectedDocId + "\t2018-1-1\t" + sectionId_1
//				+ "\tnull\tnull\tnull\tparagraph\tparagraph\ttrue\t0\t85\t\"" + paragraphText_1
//				+ "\"\tnull\tnull\tnull";
//		String expectedSerializedParagraph1Str_b = paragraphId_1 + "\t" + paragraphAnnot_1.getAnnotator().getName()
//				+ "\t" + expectedDocId + "\t2018-1-1\t" + sectionId_2
//				+ "\tnull\tnull\tnull\tparagraph\tparagraph\ttrue\t0\t85\t\"" + paragraphText_1
//				+ "\"\tnull\tnull\tnull";
//		List<String> expectedSerializedParagraph1Strs = CollectionsUtil.createList(expectedSerializedParagraph1Str_a,
//				expectedSerializedParagraph1Str_b);
//		List<String> serializedParagraph1Strs = serializer.toString(paragraphAnnot_1, DOCUMENT_TEXT);
//		assertEquals("The first paragraph should have 2 serialized strings for the 2 sections of which it is a part.",
//				2, serializedParagraph1Strs.size());
//		Collections.sort(expectedSerializedParagraph1Strs);
//		Collections.sort(serializedParagraph1Strs);
//		assertEquals(expectedSerializedParagraph1Strs, serializedParagraph1Strs);

		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedParagraph1 = serializer.toString(paragraphAnnot_1, DOCUMENT_TEXT);
		assertEquals(2, serializedParagraph1.size());

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(paragraphId_1 + "\tbioc\t" + docId + "\t"
				+ Layer.PARAGRAPH.name() + "\tparagraph\t" + paragraphAnnot_1.getAggregateSpan().getSpanStart() + "\t"
				+ paragraphAnnot_1.getAggregateSpan().getSpanEnd() + "\t\"" + paragraphText_1 + "\"");

		// paragraph1 is in two sections
		Set<String> expectedInSectionLine = CollectionsUtil.createSet(paragraphId_1 + "\t" + sectionId_1,
				paragraphId_1 + "\t" + sectionId_2);

		assertEquals(expectedAnnotationLine, serializedParagraph1.get(TableKey.ANNOTATION));
		assertEquals(expectedInSectionLine, serializedParagraph1.get(TableKey.IN_SECTION));

	}

	@Test
	public void testSentenceAnnotSerialization() {

//
//		/* test serialization of sentence 1 annotation */
//		String expectedSerializedSentence1Str_a = sentenceId_1 + "\t" + sentenceAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_1 + "\t" + paragraphId_1
//				+ "\tnull\tnull\tsentence\tsentence\ttrue\t0\t34\t\"" + sentenceText_1 + "\"\tnull\tnull\tnull";
//		String expectedSerializedSentence1Str_b = sentenceId_1 + "\t" + sentenceAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_2 + "\t" + paragraphId_1
//				+ "\tnull\tnull\tsentence\tsentence\ttrue\t0\t34\t\"" + sentenceText_1 + "\"\tnull\tnull\tnull";
//		List<String> expectedSerializedSentence1Strs = CollectionsUtil.createList(expectedSerializedSentence1Str_a,
//				expectedSerializedSentence1Str_b);
//		List<String> serializedSentence1Strs = serializer.toString(sentenceAnnot_1, DOCUMENT_TEXT);
//		assertEquals(
//				"The first sentence should have 2 serialized strings. It is part of 1 paragraph that is part of 2 sections.",
//				2, serializedSentence1Strs.size());
//		Collections.sort(expectedSerializedSentence1Strs);
//		Collections.sort(serializedSentence1Strs);
//		assertEquals(expectedSerializedSentence1Strs, serializedSentence1Strs);
//

		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedParagraph1 = serializer.toString(sentenceAnnot_1, DOCUMENT_TEXT);
		assertEquals(3, serializedParagraph1.size());

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(sentenceId_1 + "\tturku\t" + docId + "\t"
				+ Layer.SENTENCE.name() + "\tsentence\t" + sentenceAnnot_1.getAggregateSpan().getSpanStart() + "\t"
				+ sentenceAnnot_1.getAggregateSpan().getSpanEnd() + "\t\"" + sentenceText_1 + "\"");

		// paragraph1 is in two sections
		Set<String> expectedInSectionLine = CollectionsUtil.createSet(sentenceId_1 + "\t" + sectionId_1,
				sentenceId_1 + "\t" + sectionId_2);
		// sentence1 is in one paragraph
		Set<String> expectedInParagraphLine = CollectionsUtil.createSet(sentenceId_1 + "\t" + paragraphId_1);

		assertEquals(expectedAnnotationLine, serializedParagraph1.get(TableKey.ANNOTATION));
		assertEquals(expectedInSectionLine, serializedParagraph1.get(TableKey.IN_SECTION));
		assertEquals(expectedInParagraphLine, serializedParagraph1.get(TableKey.IN_PARAGRAPH));

	}

	@Test
	public void testConceptSerialization() {

		// in section1, paragraph2, sentence4

		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedConcept3 = serializer.toString(conceptAnnot_3, DOCUMENT_TEXT);
		assertEquals(4, serializedConcept3.size());

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(conceptId_3 + "\toger\t" + docId + "\t"
				+ Layer.CONCEPT.name() + "\tCL:0000000\t" + conceptAnnot_3.getAggregateSpan().getSpanStart() + "\t"
				+ conceptAnnot_3.getAggregateSpan().getSpanEnd() + "\t\"" + "concept" + "\"");

		Set<String> expectedInSectionLine = CollectionsUtil.createSet(conceptId_3 + "\t" + sectionId_1);
		Set<String> expectedInParagraphLine = CollectionsUtil.createSet(conceptId_3 + "\t" + paragraphId_2);
		Set<String> expectedInSentenceLine = CollectionsUtil.createSet(conceptId_3 + "\t" + sentenceId_4);

		assertEquals(expectedAnnotationLine, serializedConcept3.get(TableKey.ANNOTATION));
		assertEquals(expectedInSectionLine, serializedConcept3.get(TableKey.IN_SECTION));
		assertEquals(expectedInParagraphLine, serializedConcept3.get(TableKey.IN_PARAGRAPH));
		assertEquals(expectedInSentenceLine, serializedConcept3.get(TableKey.IN_SENTENCE));

	}

	@Test
	public void testTokenSerialization() {
		/*
		 * test serialization for token 'different' which is inside a concept
		 * annotation, inside a sentence, inside 1 paragraph, and inside 2 sections
		 */

		TextAnnotation tokenAnnotation_different = annotFactory.createAnnotation(67, 76, "different", "ADJ", "turku");
		String tokenId_different = BigQueryUtil.getAnnotationIdentifier(tokenAnnotation_different, Layer.TOKEN);
//
//		String expectedSerializedTokenStr_a = tokenId_different + "\t"
//				+ tokenAnnotation_different.getAnnotator().getName() + "\t" + expectedDocId + "\t2018-1-1\t"
//				+ sectionId_1 + "\t" + paragraphId_1 + "\t" + sentenceId_2 + "\t" + conceptId_2
//				+ "\ttoken\ttoken\ttrue\t67\t76\t" + "\"different\"" + "\tnull\tnull\tnull";
//		String expectedSerializedTokenStr_b = tokenId_different + "\t"
//				+ tokenAnnotation_different.getAnnotator().getName() + "\t" + expectedDocId + "\t2018-1-1\t"
//				+ sectionId_2 + "\t" + paragraphId_1 + "\t" + sentenceId_2 + "\t" + conceptId_2
//				+ "\ttoken\ttoken\ttrue\t67\t76\t" + "\"different\"" + "\tnull\tnull\tnull";
//
//		List<String> expectedSerializedTokenStrs = CollectionsUtil.createList(expectedSerializedTokenStr_a,
//				expectedSerializedTokenStr_b);
//		List<String> serializedTokenStrs = serializer.toString(tokenAnnotation_different, DOCUMENT_TEXT);
//
//		assertEquals(
//				"The first sentence should have 2 serialized strings. It is part of 1 paragraph that is part of 2 sections.",
//				2, serializedTokenStrs.size());
//
//		Collections.sort(expectedSerializedTokenStrs);
//		Collections.sort(serializedTokenStrs);
//// System.out.println();
//// System.out.println("EXP: " + expectedSerializedTokenStrs);
//// System.out.println("OBS: " + serializedTokenStrs);
//// System.out.println();
//
//		assertEquals(expectedSerializedTokenStrs, serializedTokenStrs);

		BigQueryAnnotationSerializer serializer = new BigQueryAnnotationSerializer(docId, // publicationDate,
				sectionSpanToIdMap, paragraphSpanToIdMap, sentenceSpanToIdMap, conceptSpanToIdMap, // typeToParentTypeMap,
				annotationIdToRelationMap);

		Map<TableKey, Set<String>> serializedToken = serializer.toString(tokenAnnotation_different, DOCUMENT_TEXT);
		assertEquals(5, serializedToken.size());

		Set<String> expectedAnnotationLine = CollectionsUtil.createSet(tokenId_different + "\tturku\t" + docId + "\t"
				+ Layer.TOKEN.name() + "\tADJ\t" + tokenAnnotation_different.getAggregateSpan().getSpanStart() + "\t"
				+ tokenAnnotation_different.getAggregateSpan().getSpanEnd() + "\t\"" + "different" + "\"");

		// paragraph1 is in two sections
		Set<String> expectedInSectionLine = CollectionsUtil.createSet(tokenId_different + "\t" + sectionId_1,
				tokenId_different + "\t" + sectionId_2);
		// sentence1 is in one paragraph
		Set<String> expectedInParagraphLine = CollectionsUtil.createSet(tokenId_different + "\t" + paragraphId_1);
		Set<String> expectedInSentenceLine = CollectionsUtil.createSet(tokenId_different + "\t" + sentenceId_2);
		Set<String> expectedInConceptLine = CollectionsUtil.createSet(tokenId_different + "\t" + conceptId_2);

		assertEquals(expectedAnnotationLine, serializedToken.get(TableKey.ANNOTATION));
		assertEquals(expectedInSectionLine, serializedToken.get(TableKey.IN_SECTION));
		assertEquals(expectedInParagraphLine, serializedToken.get(TableKey.IN_PARAGRAPH));
		assertEquals(expectedInSentenceLine, serializedToken.get(TableKey.IN_SENTENCE));
		assertEquals(expectedInConceptLine, serializedToken.get(TableKey.IN_CONCEPT));
	}

//
//		/*
//		 * test serialization of a concept annotation whose type has parent concepts
//		 */
//
//// TextAnnotation conceptAnnot_1_CL_0000001 =
//// annotFactory.createAnnotation(26, 33, "concept",
//// "http://purl.obolibrary.org/obo/CL_0000001",
//// "conceptmapper");
//// TextAnnotation conceptAnnot_1_CL_0000002 =
//// annotFactory.createAnnotation(26, 33, "concept",
//// "http://purl.obolibrary.org/obo/CL_0000002",
//// "conceptmapper");
//
//// String conceptId_1_CL_0000001 =
//// BigQueryUtil.getAnnotationIdentifier(conceptAnnot_1_CL_0000001,
//// "concept");
//// String conceptId_1_CL_0000002 =
//// BigQueryUtil.getAnnotationIdentifier(conceptAnnot_1_CL_0000002,
//// "concept");
//
//		String expectedSerializedConcept1Str_a = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_1 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000000\ttrue\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//		String expectedSerializedConcept1Str_b = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_2 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000000\ttrue\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//		String expectedSerializedConcept1Str_c = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_1 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000001\tfalse\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//		String expectedSerializedConcept1Str_d = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_2 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000001\tfalse\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//		String expectedSerializedConcept1Str_e = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_1 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000002\tfalse\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//		String expectedSerializedConcept1Str_f = conceptId_1 + "\t" + conceptAnnot_1.getAnnotator().getName() + "\t"
//				+ expectedDocId + "\t2018-1-1\t" + sectionId_2 + "\t" + paragraphId_1 + "\t" + sentenceId_1
//				+ "\tnull\tconcept\thttp://purl.obolibrary.org/obo/CL_0000002\tfalse\t26\t33\t" + "\"concept\""
//				+ "\tnull\tnull\tnull";
//
//		List<String> expectedSerializedConcept1Strs = CollectionsUtil.createList(expectedSerializedConcept1Str_a,
//				expectedSerializedConcept1Str_b, expectedSerializedConcept1Str_c, expectedSerializedConcept1Str_d,
//				expectedSerializedConcept1Str_e, expectedSerializedConcept1Str_f);
//		List<String> serializedConcept1Strs = serializer.toString(conceptAnnot_1, DOCUMENT_TEXT);
//		assertEquals(
//				"The concept appears in 2 sections, so there should be 2 annotations for each type. With the parent types there are 3 total types, so there should be 6 annotations",
//				6, serializedConcept1Strs.size());
//		Collections.sort(expectedSerializedConcept1Strs);
//		Collections.sort(serializedConcept1Strs);
//
//// System.out.println();
//// for (String s : expectedSerializedConcept1Strs) {
//// System.out.println("EXP: " + s);
//// }
//// for (String s : serializedConcept1Strs) {
//// System.out.println("OBS: " + s);
//// }
//// System.out.println();
//
//		assertEquals(expectedSerializedConcept1Strs, serializedConcept1Strs);
//
//		/*
//		 * test serialization for token which is inside a concept annotation, inside a
//		 * sentence, inside 1 paragraph, and inside 2 sections, and the concept
//		 * annotation class has two parent classes
//		 */
//
//		TextAnnotation tokenAnnotation_concept = annotFactory.createAnnotation(26, 33, "concept", "token", "opennlp");
//		String tokenId_concept = BigQueryUtil.getAnnotationIdentifier(tokenAnnotation_concept, "token");
//
//		String expectedSerializedTokenStr_concept_a = tokenId_concept + "\t"
//				+ tokenAnnotation_concept.getAnnotator().getName() + "\t" + expectedDocId + "\t2018-1-1\t" + sectionId_1
//				+ "\t" + paragraphId_1 + "\t" + sentenceId_1 + "\t" + conceptId_1 + "\ttoken\ttoken\ttrue\t26\t33\t"
//				+ "\"concept\"" + "\tnull\tnull\tnull";
//
//		String expectedSerializedTokenStr_concept_b = tokenId_concept + "\t"
//				+ tokenAnnotation_concept.getAnnotator().getName() + "\t" + expectedDocId + "\t2018-1-1\t" + sectionId_2
//				+ "\t" + paragraphId_1 + "\t" + sentenceId_1 + "\t" + conceptId_1 + "\ttoken\ttoken\ttrue\t26\t33\t"
//				+ "\"concept\"" + "\tnull\tnull\tnull";
//
//		List<String> expectedSerializedTokenStrs_concept = CollectionsUtil
//				.createList(expectedSerializedTokenStr_concept_a, expectedSerializedTokenStr_concept_b);
//		List<String> serializedTokenStrs_concept = serializer.toString(tokenAnnotation_concept, DOCUMENT_TEXT);
//
//		assertEquals(
//				"The token should have 2 serialized strings. It overlaps a single concept but appears in 2 sections.",
//				2, serializedTokenStrs_concept.size());
//
//		Collections.sort(expectedSerializedTokenStrs_concept);
//		Collections.sort(serializedTokenStrs_concept);
//		System.out.println();
//		for (String s : expectedSerializedTokenStrs_concept) {
//			System.out.println("EXP: " + s);
//		}
//		for (String s : serializedTokenStrs_concept) {
//			System.out.println("OBS: " + s);
//		}
//		System.out.println();
//
//		assertEquals(expectedSerializedTokenStrs_concept, serializedTokenStrs_concept);
//
//	}
	
	@Test
	public void testDetermineLayer() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");

		assertEquals(Layer.CONCEPT, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "CL:0000000", "oger")));
		assertEquals(Layer.PARAGRAPH, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "paragraph", "bioc")));
		assertEquals(Layer.SENTENCE, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "sentence", "turku")));
		assertEquals(Layer.TOKEN, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "VERB", "turku")));
		assertEquals(Layer.SENTENCE, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "reference", "bioc")));
		assertEquals(Layer.SECTION, BigQueryAnnotationSerializer
				.determineLayer(factory.createAnnotation(0, 5, "Hello", "TITLE", "bioc")));
	}
	

}