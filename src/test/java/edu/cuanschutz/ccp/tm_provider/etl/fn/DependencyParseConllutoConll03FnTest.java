package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class DependencyParseConllutoConll03FnTest {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	private static final String TEXT_FILE_NAME = "PMID11597317.txt";
	private static final String DEPENDENCY_PARSE_FILE_NAME = "PMID11597317.dependency_parse.conllu";

	@Test
	public void testFetTokensAsConll03() throws IOException {

		String docId = "11597317";

		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), TEXT_FILE_NAME, ENCODING);
		String conllu = ClassPathUtil.getContentsFromClasspathResource(getClass(), DEPENDENCY_PARSE_FILE_NAME,
				ENCODING);
		String conll03 = DependencyParseConlluToConll03Fn.getTokensAsConll03(docId, docText, conllu);

		StringBuilder expectedBeginningConll03 = new StringBuilder();
		expectedBeginningConll03
				.append(String.format("%s\t%s\n", DependencyParseConlluToConll03Fn.DOCUMENT_ID_LINE_PREFIX, docId));
		expectedBeginningConll03.append("BRCA2\n");
		expectedBeginningConll03.append("and\n");
		expectedBeginningConll03.append("homologous\n");
		expectedBeginningConll03.append("recombination\n");
		expectedBeginningConll03.append("\n");
		expectedBeginningConll03.append("Abstract\n");
		expectedBeginningConll03.append("\n");
		expectedBeginningConll03.append("Two\n");
		expectedBeginningConll03.append("recent\n");
		expectedBeginningConll03.append("papers\n");
		expectedBeginningConll03.append("provide\n");
		expectedBeginningConll03.append("new\n");

		// test that the beginning of the CoNLL '03 text is as expected
		assertTrue(conll03.startsWith(expectedBeginningConll03.toString()));
	}

}
