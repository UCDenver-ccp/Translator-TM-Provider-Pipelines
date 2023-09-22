package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.BiocToTextFnTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class SentenceCooccurrenceBuilderTest {

	private Map<DocumentType, String> docTypeToContent;
	private String conceptChebi;
	private String conceptCl;
	private String dependency;
	private String sentences;
	private String text;

	@Before
	public void setUp() throws IOException {
		docTypeToContent = new HashMap<DocumentType, String>();
		conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.concept_chebi.bionlp",
				CharacterEncoding.UTF_8);
		conceptCl = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.concept_cl.bionlp",
				CharacterEncoding.UTF_8);
		dependency = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.dependency.conllu",
				CharacterEncoding.UTF_8);
		sentences = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.sentence.bionlp",
				CharacterEncoding.UTF_8);
		text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.txt", CharacterEncoding.UTF_8);

		docTypeToContent.put(DocumentType.TEXT, text);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.DEPENDENCY_PARSE, dependency);
	}

	@Test
	public void testIsAsExpected() throws IOException {
		String version1 = ClassPathUtil.getContentsFromClasspathResource(BiocToTextFnTest.class, "PMC1790863.txt",
				CharacterEncoding.UTF_8);
		assertEquals(version1.trim(), text.trim());
	}

	@Test
	public void testExtractAllAnnotations_concept() throws IOException {

		String docId = "12345";

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = SentenceCooccurrenceBuilder.extractAllAnnotations(docId, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		// fewer after excluding _EXT
		assertEquals("there should be 5 CL annotations and 65 CHEBI annotations", 18, td.getAnnotations().size());

		for (TextAnnotation ta : td.getAnnotations()) {
			String substring = td.getText().substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());

			System.out.println("SUBSTRING: " + substring + " --- " + ta.getCoveredText() + " --- "
					+ ta.getAggregateSpan().toString());
			assertEquals(ta.getCoveredText(),
					text.substring(ta.getAggregateSpan().getSpanStart(), ta.getAggregateSpan().getSpanEnd()));

			assertFalse(substring.startsWith(" "));
		}

	}

	@Test
	public void testExtractAllAnnotations_dependency() throws IOException {

		String docId = "12345";

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.DEPENDENCY_PARSE, dependency);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = SentenceCooccurrenceBuilder.extractAllAnnotations(docId, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		assertEquals("there should be 319 sentence annotations", 319, td.getAnnotations().size());

		for (TextAnnotation ta : td.getAnnotations()) {
			String substring = td.getText().substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());
			if (StringUtil.startsWithRegex(substring, "\\s")) {
				fail("sstarts with space: " + ta.getAggregateSpan() + " -- " + substring);
			}
		}

		// check for dependency relations
		Set<String> slots = new HashSet<String>();
		for (TextAnnotation ta : td.getAnnotations()) {
//			System.out.println(ta);
			Collection<String> slotNames = ta.getClassMention().getComplexSlotMentionNames();
			slots.addAll(slotNames);
		}
		System.out.println(slots);

	}

	@Test
	public void testToSentenceCooccurrenceString_sentencesFromDependencyParse() throws IOException {
		String docId = "12345";
		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();
		String sentCooccurStr = builder.toSentenceCooccurrenceString(docId, docTypeToContent);

		assertFalse(sentCooccurStr.isEmpty());
		String[] lines = sentCooccurStr.split("\\n");
		for (String line : lines) {
			System.out.println(line);
		}

	}

	@Test
	public void testToSentenceCooccurrenceString_sentencesFromSentenceData() throws IOException {
		String docId = "12345";
		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();

		docTypeToContent.remove(DocumentType.DEPENDENCY_PARSE);
		docTypeToContent.put(DocumentType.SENTENCE, sentences);

		String sentCooccurStr = builder.toSentenceCooccurrenceString(docId, docTypeToContent);

		assertFalse(sentCooccurStr.isEmpty());
		String[] lines = sentCooccurStr.split("\\n");
		for (String line : lines) {
			System.out.println(line);
		}

	}

	@Test
	public void testToSentenceCooccurrenceString_sentencesFromSentenceData_7890() throws IOException {
		String docId = "7890";
		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();

		Map<DocumentType, String> docTypeToContentMap = new HashMap<DocumentType, String>();
		String conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_chebi.bionlp",
				CharacterEncoding.UTF_8);
		String conceptCl = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_cl.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoBp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_go_bp.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoCc = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_go_cc.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoMf = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_go_mf.bionlp",
				CharacterEncoding.UTF_8);
		String conceptMop = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_mop.bionlp",
				CharacterEncoding.UTF_8);
		String conceptNcbitaxon = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"7890.concept_ncbitaxon.bionlp", CharacterEncoding.UTF_8);
		String conceptPr = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_pr.bionlp",
				CharacterEncoding.UTF_8);
		String conceptSo = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_so.bionlp",
				CharacterEncoding.UTF_8);
		String conceptUberon = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.concept_uberon.bionlp",
				CharacterEncoding.UTF_8);

		String sentences = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.sentences.bionlp",
				CharacterEncoding.UTF_8);
		String text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "7890.txt", CharacterEncoding.UTF_8);

		docTypeToContentMap.put(DocumentType.TEXT, text);
		docTypeToContentMap.put(DocumentType.SENTENCE, sentences);
		docTypeToContentMap.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContentMap.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_BP, conceptGoBp);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_CC, conceptGoCc);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_MF, conceptGoMf);
		docTypeToContentMap.put(DocumentType.CONCEPT_MOP, conceptMop);
		docTypeToContentMap.put(DocumentType.CONCEPT_NCBITAXON, conceptNcbitaxon);
		docTypeToContentMap.put(DocumentType.CONCEPT_PR, conceptPr);
		docTypeToContentMap.put(DocumentType.CONCEPT_SO, conceptSo);
		docTypeToContentMap.put(DocumentType.CONCEPT_UBERON, conceptUberon);

		String sentCooccurStr = builder.toSentenceCooccurrenceString(docId, docTypeToContentMap);

		assertFalse(sentCooccurStr.isEmpty());
		String[] lines = sentCooccurStr.split("\\n");
		for (String line : lines) {
			System.out.println(line);
		}

	}

	@Test
	public void testToSentenceCooccurrenceString_sentencesFromSentenceData_PMC2570843_xml() throws IOException {
		String docId = "567";
		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();

		Map<DocumentType, String> docTypeToContentMap = new HashMap<DocumentType, String>();
		String conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_chebi.bionlp",
				CharacterEncoding.UTF_8);
		String conceptCl = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_cl.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoBp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_go_bp.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoCc = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_go_cc.bionlp",
				CharacterEncoding.UTF_8);
		String conceptGoMf = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_go_mf.bionlp",
				CharacterEncoding.UTF_8);
		String conceptMop = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_mop.bionlp",
				CharacterEncoding.UTF_8);
		String conceptNcbitaxon = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"567.concept_ncbitaxon.bionlp", CharacterEncoding.UTF_8);
		String conceptPr = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_pr.bionlp",
				CharacterEncoding.UTF_8);
		String conceptSo = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_so.bionlp",
				CharacterEncoding.UTF_8);
		String conceptUberon = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_uberon.bionlp",
				CharacterEncoding.UTF_8);

		String sentences = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.sentences.bionlp",
				CharacterEncoding.UTF_8);
		String text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.txt", CharacterEncoding.UTF_8);

		docTypeToContentMap.put(DocumentType.TEXT, text);
		docTypeToContentMap.put(DocumentType.SENTENCE, sentences);
		docTypeToContentMap.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContentMap.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_BP, conceptGoBp);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_CC, conceptGoCc);
		docTypeToContentMap.put(DocumentType.CONCEPT_GO_MF, conceptGoMf);
		docTypeToContentMap.put(DocumentType.CONCEPT_MOP, conceptMop);
		docTypeToContentMap.put(DocumentType.CONCEPT_NCBITAXON, conceptNcbitaxon);
		docTypeToContentMap.put(DocumentType.CONCEPT_PR, conceptPr);
		docTypeToContentMap.put(DocumentType.CONCEPT_SO, conceptSo);
		docTypeToContentMap.put(DocumentType.CONCEPT_UBERON, conceptUberon);

		String sentCooccurStr = builder.toSentenceCooccurrenceString(docId, docTypeToContentMap);

		assertFalse(sentCooccurStr.isEmpty());
		String[] lines = sentCooccurStr.split("\\n");
		for (String line : lines) {
			System.out.println(line);
		}

	}

	@Test
	public void testToOgerAnnotAlignment() throws IOException {
//		String docId = "567";
//		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();

		Map<DocumentType, String> docTypeToContentMap = new HashMap<DocumentType, String>();
		String conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_chebi.bionlp",
				CharacterEncoding.UTF_8);
		String sentences = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.sentences.bionlp",
				CharacterEncoding.UTF_8);
		String text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.txt", CharacterEncoding.UTF_8);

		docTypeToContentMap.put(DocumentType.TEXT, text);
		docTypeToContentMap.put(DocumentType.SENTENCE, sentences);
		docTypeToContentMap.put(DocumentType.CONCEPT_CHEBI, conceptChebi);

		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		TextDocument td = reader.readDocument("12345", "unknown", new ByteArrayInputStream(conceptChebi.getBytes()),
				new ByteArrayInputStream(text.getBytes()), CharacterEncoding.UTF_8);

		for (TextAnnotation ta : td.getAnnotations()) {

			byte[] byteSubset = Arrays.copyOfRange(text.getBytes(), ta.getAnnotationSpanStart(),
					ta.getAnnotationSpanEnd());

			int updatedSpanStart = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanStart()))
					.length();
			int updatedSpanEnd = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanEnd())).length();

			assertEquals(new String(byteSubset), text.substring(updatedSpanStart, updatedSpanEnd));
			System.out.println(text.substring(updatedSpanStart, updatedSpanEnd) + " ====== " + new String(byteSubset)
					+ " ==== " + ta.getCoveredText() + " ----- "
					+ text.substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd()));

		}
	}

	@Test
	public void testToOgerAnnotAlignment2() throws IOException {
//		String docId = "567";
//		SentenceCooccurrenceBuilder builder = new SentenceCooccurrenceBuilder();

//		Map<DocumentType, String> docTypeToContentMap = new HashMap<DocumentType, String>();
//		String conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.concept_chebi.bionlp",
//				CharacterEncoding.UTF_8);
//		String sentences = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.sentences.bionlp",
//				CharacterEncoding.UTF_8);
//		String text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "567.txt", CharacterEncoding.UTF_8);
//
//		docTypeToContentMap.put(DocumentType.TEXT, text);
//		docTypeToContentMap.put(DocumentType.SENTENCE, sentences);
//		docTypeToContentMap.put(DocumentType.CONCEPT_CHEBI, conceptChebi);

		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		TextDocument td = reader.readDocument("12345", "unknown", new ByteArrayInputStream(conceptChebi.getBytes()),
				new ByteArrayInputStream(text.getBytes()), CharacterEncoding.UTF_8);

		for (TextAnnotation ta : td.getAnnotations()) {
			byte[] byteSubset = Arrays.copyOfRange(text.getBytes(), ta.getAnnotationSpanStart(),
					ta.getAnnotationSpanEnd());

			int updatedSpanStart = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanStart()))
					.length();
			int updatedSpanEnd = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanEnd())).length();

//			System.out.println("span before: " + ta.getAggregateSpan().toString() + " ----> " + new Span(updatedSpanStart, updatedSpanEnd).toString());

//			assertEquals("cell",text.substring(updatedSpanStart, updatedSpanEnd) );
			System.out.println(text.substring(updatedSpanStart, updatedSpanEnd) + " ====== " + new String(byteSubset)
					+ " ==== " + ta.getCoveredText() + " ----- "
					+ text.substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd()));

		}
	}

}
