package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverterTest;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.TableKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class BigQueryLoadBuilderTest {

	private Map<DocumentType, String> docTypeToContent;
	private String conceptChebi;
	private String conceptCl;
	private String dependency;
	private String sections;
	private String text;
	private String sentence;

	@Before
	public void setUp() throws IOException {
		docTypeToContent = new HashMap<DocumentType, String>();
		conceptChebi = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.concept_chebi.bionlp",
				CharacterEncoding.UTF_8);
		conceptCl = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.concept_cl.bionlp",
				CharacterEncoding.UTF_8);
		dependency = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.dependency.conllu",
				CharacterEncoding.UTF_8);
		sections = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.sections.bionlp",
				CharacterEncoding.UTF_8);
		sentence = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.sentence.bionlp",
				CharacterEncoding.UTF_8);
		text = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.txt", CharacterEncoding.UTF_8);

		docTypeToContent.put(DocumentType.TEXT, text);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.DEPENDENCY_PARSE, dependency);
		docTypeToContent.put(DocumentType.SECTIONS, sections);

	}

	@Test
	public void testIsAsExpected() throws IOException {
		String version1 = ClassPathUtil.getContentsFromClasspathResource(BiocToTextConverterTest.class,
				"PMC1790863.txt", CharacterEncoding.UTF_8);
		assertEquals(version1.trim(), text.trim());
	}

	@Test
	public void testExtractAllAnnotations_concept() throws IOException {

		String docId = "12345";
		String sourceStrContainsYear = null;

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = BigQueryLoadBuilder.extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		assertEquals("there should be 5 CL annotations and 65 CHEBI annotations", 70, td.getAnnotations().size());

		for (TextAnnotation ta : td.getAnnotations()) {
			String substring = td.getText().substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());
			assertEquals(ta.getCoveredText(),
					text.substring(ta.getAggregateSpan().getSpanStart(), ta.getAggregateSpan().getSpanEnd()));
			assertFalse(substring.startsWith(" "));
		}

	}

	@Test
	public void testExtractAllAnnotations_sentence() throws IOException {

		String docId = "12345";
		String sourceStrContainsYear = null;

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.SENTENCE, sentence);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = BigQueryLoadBuilder.extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		assertEquals("there should be 266 sentence annotations", 266, td.getAnnotations().size());

		for (TextAnnotation ta : td.getAnnotations()) {
			String substring = td.getText().substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());
			assertEquals(ta.getCoveredText(),
					text.substring(ta.getAggregateSpan().getSpanStart(), ta.getAggregateSpan().getSpanEnd())
							.replaceAll("\\n", " "));
			assertFalse(substring.startsWith(" "));
		}

	}

	@Ignore("span issue -- need to fix")
	@Test
	public void testExtractAllAnnotations_section() throws IOException {

		String docId = "12345";
		String sourceStrContainsYear = null;

		System.out.println(sections);

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.SECTIONS, sections);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = BigQueryLoadBuilder.extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		assertEquals("there should be 131 section annotations", 131, td.getAnnotations().size());

		for (TextAnnotation ta : td.getAnnotations()) {

			String substring = td.getText().substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());
			if (StringUtil.startsWithRegex(substring, "\\s")) {
				throw new RuntimeException("SSSSSSSSS");
			}
			assertEquals(ta.getCoveredText(),
					text.substring(ta.getAggregateSpan().getSpanStart(), ta.getAggregateSpan().getSpanEnd()));
			assertFalse(substring.startsWith(" "));
			assertFalse(ta.getCoveredText().startsWith(" "));
		}

	}

	@Test
	public void testExtractAllAnnotations_dependency() throws IOException {

		String docId = "12345";
		String sourceStrContainsYear = null;

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.DEPENDENCY_PARSE, dependency);
		docTypeToContent.put(DocumentType.TEXT, text);

		TextDocument td = BigQueryLoadBuilder.extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);
		assertEquals(docId, td.getSourceid());
		assertEquals("there should be 7687 token annotations", 7687, td.getAnnotations().size());

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

	@Ignore("span issue - need to fix")
	@Test
	public void testToBigQueryLoadString() throws IOException {
		String docId = "12345";
		String sourceStrContainsYear = null;
		BigQueryLoadBuilder writer = new BigQueryLoadBuilder();
		Map<TableKey, String> tables = writer.toBigQueryString(docId, sourceStrContainsYear, docTypeToContent);

//		  tables.entrySet().forEach(e -> System.out.println(e.getKey() + "\n" + e.getValue() + "\n--------------------------\n"));

		String s = tables.get(TableKey.RELATION);
		assertFalse(s.isEmpty());
		String[] lines = s.split("\\n");
		for (int i = 0; i < 25; i++) {
			System.out.println(lines[i]);
		}

	}

	@Test
	public void testToBigQueryLoadString_sentences_instead_of_dependency_parse() throws IOException {
		String docId = "12345";
		String sourceStrContainsYear = null;

		docTypeToContent = new HashMap<DocumentType, String>();
		docTypeToContent.put(DocumentType.TEXT, text);
		docTypeToContent.put(DocumentType.CONCEPT_CHEBI, conceptChebi);
		docTypeToContent.put(DocumentType.CONCEPT_CL, conceptCl);
		docTypeToContent.put(DocumentType.SENTENCE, sentence);

		BigQueryLoadBuilder writer = new BigQueryLoadBuilder();
		Map<TableKey, String> tables = writer.toBigQueryString(docId, sourceStrContainsYear, docTypeToContent);

//		  tables.entrySet().forEach(e -> System.out.println(e.getKey() + "\n" + e.getValue() + "\n--------------------------\n"));

		String s = tables.get(TableKey.IN_SENTENCE);
		assertFalse(s.isEmpty());
		String[] lines = s.split("\\n");
		for (int i = 0; i < 25; i++) {
			System.out.println(lines[i]);
		}

	}

}
