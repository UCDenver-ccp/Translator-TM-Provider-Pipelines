package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;

public class DependencyParseConlluToSentenceFnTest {
	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	private static final String TEXT_FILE_NAME = "PMID11597317.txt";
	private static final String DEPENDENCY_PARSE_FILE_NAME = "PMID11597317.dependency_parse.conllu";

	@Test
	public void testExtractSentencesFromCoNLLU() throws IOException {

		String docId = "11597317";

		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), TEXT_FILE_NAME, ENCODING);
		String conllu = ClassPathUtil.getContentsFromClasspathResource(getClass(), DEPENDENCY_PARSE_FILE_NAME,
				ENCODING);
		TextDocument td = DependencyParseConlluToSentenceFn.extractSentencesFromCoNLLU(docId, docText, conllu);

		int expectedSentenceCount = 108;
		assertEquals(expectedSentenceCount, td.getAnnotations().size());
	}

	@Test
	public void testGetSentencesAsBionlp() throws IOException {

		String docId = "11597317";

		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), TEXT_FILE_NAME, ENCODING);
		String conllu = ClassPathUtil.getContentsFromClasspathResource(getClass(), DEPENDENCY_PARSE_FILE_NAME,
				ENCODING);
		String sentencesAsBioNLP = DependencyParseConlluToSentenceFn.getSentencesAsBioNLP(docId, docText, conllu);

		BioNLPDocumentReader bionlpDocReader = new BioNLPDocumentReader();
		TextDocument td = bionlpDocReader.readDocument(docId, "unknown",
				new ByteArrayInputStream(sentencesAsBioNLP.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);

		int expectedSentenceCount = 108;
		assertEquals(expectedSentenceCount, td.getAnnotations().size());
	}

}
