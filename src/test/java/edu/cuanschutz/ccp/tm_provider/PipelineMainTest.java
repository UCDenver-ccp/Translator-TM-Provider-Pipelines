package edu.cuanschutz.ccp.tm_provider;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;

public class PipelineMainTest {

	@Test
	public void testChunkString() throws UnsupportedEncodingException {
		StringBuffer s = new StringBuffer();

		for (int i = 0; i < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES; i++) {
			s.append("a");
		}

		assertEquals("equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// add one more character and it should now return 2 strings
		s.append("b");
		assertEquals("equal to the threshold, so should be two strings", 2,
				PipelineMain.chunkContent(s.toString()).size());

		// remove two characters (in this case the first and second) and it should
		// return to 1 string
		s = s.deleteCharAt(0);
		s = s.deleteCharAt(0);
		assertEquals("back to equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// we are now at one short of the threshold. Add a UTF-8 char that is multi-byte
		// and it should jump to two strings
		s.append("\u0190");
		assertEquals(
				"equal to the threshold in character count, but greater than the byte count threshold, so should be two strings",
				2, PipelineMain.chunkContent(s.toString()).size());

	}

}
