package edu.cuanschutz.ccp.tm_provider.etl.util.helper;

import java.util.Base64;

/**
 * Utility class for decoding content stored in DataStore. Useful for debugging
 * purposes.
 *
 */
public class BlobReader {

	public static void main(String[] args) {
		String blob = "";

		byte[] decodedBytes = Base64.getDecoder().decode(blob);
		String decodedString = new String(decodedBytes);
		System.out.println(decodedString);
	}
}
