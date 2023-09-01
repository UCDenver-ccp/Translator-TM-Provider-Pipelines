package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

/**
 * This class creates a dictionary that contains some utility annotations -
 * initially, there is one utility annotation that will mark the end of official
 * document text. This will allow for additional text to be added at the end of
 * the document so that it can be processed by OGER and other components.
 * Initial use of this extra text pertains to abbreviation handling where we
 * want to process sentences that contain abbreviation definitions after the
 * short form of the abbreviation definition have been remove.
 *
 */
public class UtilityOgerDictFileFactory extends OgerDictFileFactory {

	public static final String DOCUMENT_END_MARKER_ID = "TMKPUTIL:1";
	public static final String DOCUMENT_END_MARKER = "zzzDOCUMENTzENDzzz";

	public UtilityOgerDictFileFactory() {
		super("util", null, null, null);
	}

	/**
	 * All utility annotations will be case-sensitive
	 */
	@Override
	public void createOgerDictionaryFile(File noInputFileNeeded, File dictDirectory) throws IOException {
		File caseSensitiveDictFile = new File(dictDirectory, "UTILITY.case_sensitive.tsv");

		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile)) {

			String dictLine = OgerDictFileFactory.getDictLine("Utility", DOCUMENT_END_MARKER_ID, DOCUMENT_END_MARKER,
					DOCUMENT_END_MARKER, "util", false, null);
			OgerDictFileFactory.writeDictLine(new HashSet<String>(), caseSensWriter, dictLine);
		}
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		return syns;
	}

}
