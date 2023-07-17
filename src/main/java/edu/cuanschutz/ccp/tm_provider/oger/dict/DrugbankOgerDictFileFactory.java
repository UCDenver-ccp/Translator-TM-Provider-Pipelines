package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord.Synonym;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugbankXmlFileRecordReader;

/**
 * NOTE: there was a POM exclusion necessary in order for this to run (see the
 * pom file - woodstox was excluded in two places), however excluding the
 * library causes test failures, so for now they are commented out.
 *
 */
public class DrugbankOgerDictFileFactory extends OgerDictFileFactory {

	public DrugbankOgerDictFileFactory() {
		super("drug", "DRUGBANK", SynonymSelection.EXACT_ONLY, null);
	}

	@Override
	public void createOgerDictionaryFile(File drugbankXmlFile, File dictDirectory) throws IOException {
		File caseInsensitiveDictFile = new File(dictDirectory, "DRUGBANK.case_insensitive.tsv");
		File caseSensitiveDictFile = new File(dictDirectory, "DRUGBANK.case_sensitive.tsv");

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensWriter = FileWriterUtil.initBufferedWriter(caseInsensitiveDictFile)) {
			for (DrugbankXmlFileRecordReader rr = new DrugbankXmlFileRecordReader(drugbankXmlFile); rr.hasNext();) {
				DrugBankDrugRecord record = rr.next();

				String drugbankId = "DRUGBANK:" + record.getDrugBankId().getId();
				String drugName = record.getDrugName();
				Set<Synonym> synonyms = record.getSynonyms();

				Set<String> syns = new HashSet<String>();
				syns.add(drugName);
				if (synonyms != null) {
					for (Synonym synonym : synonyms) {
						syns.add(synonym.getSynonym());
					}
				}

				syns = augmentSynonyms(drugbankId, syns);
				/*
				 * split the synonyms into two sets, one that will be match in a case sensitive
				 * manner, and one that will be case insensitive
				 */
				Set<String> caseSensitiveSyns = OgerDictFileFactory.getCaseSensitiveSynonyms(syns);

				/* the synonyms set becomes the case-insensitive set */
				syns.removeAll(caseSensitiveSyns);

				for (String synonym : caseSensitiveSyns) {
					String dictLine = OgerDictFileFactory.getDictLine("DrugBank", drugbankId, synonym, drugName, "drug",
							false);
					OgerDictFileFactory.writeDictLine(alreadyWritten, caseSensWriter, dictLine);
				}

				for (String synonym : syns) {
					String dictLine = OgerDictFileFactory.getDictLine("DrugBank", drugbankId, synonym, drugName, "drug",
							false);
					OgerDictFileFactory.writeDictLine(alreadyWritten, caseInsensWriter, dictLine);
				}
			}
		}
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
