package edu.cuanschutz.ccp.tm_provider.etl.util.helper;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

import edu.ucdenver.ccp.common.file.CharacterEncoding;

/**
 * Utility class for decoding content stored in DataStore. Useful for debugging
 * purposes.
 *
 */
public class BlobReader {

	public static void main(String[] args) throws UnsupportedEncodingException {
		String blob = "bm90IHN1cmUgLS0gbnVsbCBUaHJvd2FibGUu ".trim();

		byte[] decodedBytes = Base64.getDecoder().decode(blob);
		String decodedString = new String(decodedBytes, CharacterEncoding.UTF_8.getCharacterSetName());
		System.out.println(decodedString);
	}
}
