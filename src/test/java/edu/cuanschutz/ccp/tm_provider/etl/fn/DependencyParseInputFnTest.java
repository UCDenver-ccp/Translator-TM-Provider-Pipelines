package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DependencyParseImportFn.BulkConlluIterator;
import edu.ucdenver.ccp.common.io.ClassPathUtil;

public class DependencyParseInputFnTest {

	private static final String SAMPLE_BULK_CONLLU_FILE_NAME = "sample.BATCH_1.dependency_parses.conllu.gz";

	@Test
	public void testBulkConlluIterator() throws IOException {

		InputStream inputStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(),
				SAMPLE_BULK_CONLLU_FILE_NAME);

		BulkConlluIterator conlluIter = new BulkConlluIterator(new GZIPInputStream(inputStream));

		assertTrue(conlluIter.hasNext());
		assertEquals("PMID:28258677", conlluIter.getDocId());
		assertEquals(new HashSet<String>(Arrays.asList("PUBMED_SUB_28", "TEST")), conlluIter.getCollections());
		String conllu = conlluIter.next();
		assertTrue(countLines(conllu) > 50);

		assertTrue(conlluIter.hasNext());
		assertEquals("PMID:2", conlluIter.getDocId());
		assertEquals(new HashSet<String>(Arrays.asList("PUBMED_SUB_2", "TEST", "TEST2")), conlluIter.getCollections());
		conllu = conlluIter.next();
		assertTrue(countLines(conllu) > 20);

		assertFalse(conlluIter.hasNext());

	}

	/**
	 * @param s
	 * @return the number of lines in the string
	 */
	private int countLines(String s) {
		int count = s.split("\\n").length;
		return count;
	}

}
