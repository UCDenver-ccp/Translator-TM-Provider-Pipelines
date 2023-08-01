package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord.Synonym;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugbankXmlFileRecordReader;

/**
 * TODO: implement this - from Drugbank XML file
 *
 */
public class ProcedureOgerDictFileFactory extends OgerDictFileFactory {

	private static final String PROCEDURE_SNOMED_ID = "71388002";

	private final Set<String> procedureIdentifiers;

	public ProcedureOgerDictFileFactory(File snomedTransitiveSubclassRelationsFile)
			throws FileNotFoundException, IOException {
		super("procedure", "SNOMEDCT", SynonymSelection.EXACT_ONLY, null);
		procedureIdentifiers = loadIdentifiers(snomedTransitiveSubclassRelationsFile);
	}

	/**
	 * procedure identifiers are in the 2nd column
	 * 
	 * @param snomedTransitiveSubclassRelationsFile
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private Set<String> loadIdentifiers(File snomedTransitiveSubclassRelationsFile)
			throws FileNotFoundException, IOException {

		Set<String> procedureIds = new HashSet<String>();
		StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(snomedTransitiveSubclassRelationsFile)),
				CharacterEncoding.UTF_8, "id\teffectiveTime");
		while (lineIter.hasNext()) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 100000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}
			String[] cols = line.getText().split("\\t");
			String superTypeId = cols[0];
			if (superTypeId.equals(PROCEDURE_SNOMED_ID)) {
				String subTypeId = cols[1];
				procedureIds.add(subTypeId);
			}
		}

		return procedureIds;
	}

	@Override
	public void createOgerDictionaryFile(File snomedDescriptionsFile, File dictDirectory) throws IOException {

		File caseInsensitiveDictFile = new File(dictDirectory, "SNOMEDCT_PROCEDURES.case_insensitive.tsv");
		File caseSensitiveDictFile = new File(dictDirectory, "SNOMEDCT_PROCEDURES.case_sensitive.tsv");

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensWriter = FileWriterUtil.initBufferedWriter(caseInsensitiveDictFile)) {

			StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(snomedDescriptionsFile)), CharacterEncoding.UTF_8,
					"id\teffectiveTime");
			String previousConceptId = null;
			Set<String> synonyms = new HashSet<String>();
			while (lineIter.hasNext()) {
				Line line = lineIter.next();
				if (line.getLineNumber() % 100000 == 0) {
					System.out.println(String.format("progress: %d", line.getLineNumber()));
				}
				String[] cols = line.getText().split("\\t");
				String conceptId = cols[4];
				if (previousConceptId == null) {
					previousConceptId = conceptId;
				}
				if (!conceptId.equals(previousConceptId)) {

					writeSynonymRecord(alreadyWritten, caseSensWriter, caseInsensWriter, previousConceptId, synonyms);
					synonyms = new HashSet<String>();
					previousConceptId = conceptId;

				}

				String synonym = cols[7];
				synonyms.add(synonym);

			}
			// write final concept (potentially)
			writeSynonymRecord(alreadyWritten, caseSensWriter, caseInsensWriter, previousConceptId, synonyms);
		}
	}

	/**
	 * If this is a procedure concept, then write the synonyms to the OGER dict
	 * files
	 * 
	 * @param alreadyWritten
	 * @param caseSensWriter
	 * @param caseInsensWriter
	 * @param previousConceptId
	 * @param synonyms
	 * @param conceptId
	 * @throws IOException
	 */
	private void writeSynonymRecord(Set<String> alreadyWritten, BufferedWriter caseSensWriter,
			BufferedWriter caseInsensWriter, String previousConceptId, Set<String> synonyms) throws IOException {
		if (procedureIdentifiers.contains(previousConceptId)) {

			// this is randomly selected b/c it doesn't matter what the name is -- just
			// needs to be a filler in the OGER dict file
			String randomName = synonyms.iterator().next();

			// then output the synonyms to file b/c this concept is a procedure
			synonyms = augmentSynonyms(previousConceptId, synonyms);
			/*
			 * split the synonyms into two sets, one that will be match in a case sensitive
			 * manner, and one that will be case insensitive
			 */
			Set<String> caseSensitiveSyns = OgerDictFileFactory.getCaseSensitiveSynonyms(synonyms);

			/* the synonyms set becomes the case-insensitive set */
			synonyms.removeAll(caseSensitiveSyns);

			for (String synonym : caseSensitiveSyns) {
				String dictLine = OgerDictFileFactory.getDictLine("SNOMEDCT", "SNOMEDCT:" + previousConceptId, synonym,
						randomName, "procedure", false);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseSensWriter, dictLine);
			}

			for (String synonym : synonyms) {
				String dictLine = OgerDictFileFactory.getDictLine("SNOMEDCT", "SNOMEDCT:" + previousConceptId, synonym,
						randomName, "procedure", false);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseInsensWriter, dictLine);
			}
		}
	}

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList());

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

}
