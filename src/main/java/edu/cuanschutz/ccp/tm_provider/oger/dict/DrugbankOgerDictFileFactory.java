package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord.Synonym;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugbankXmlFileRecordReader;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

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

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList("DRUGBANK:DB10415", // Rabbit
			"DRUGBANK:DB10633", // Fig
			"DRUGBANK:DB14245", // Snail
			"DRUGBANK:DB14244", // Snail
			"DRUGBANK:DB10509", // Beef
			"DRUGBANK:DB10551", // Pea
			"DRUGBANK:DB09393", // amino acids
			"DRUGBANK:DB09145", // Water
			"DRUGBANK:DB04540", // Cholesterol
			"DRUGBANK:DB11017", // Yeast
			"DRUGBANK:DB10632", // Date
			"DRUGBANK:DB12938", // Balance
			"DRUGBANK:DB10519", // Chicken
			"DRUGBANK:DB02891", // Beam
			"DRUGBANK:DB11577", // blue, x
			"DRUGBANK:DB10549", // orange extract
			"DRUGBANK:DB10537", // Lamb
			"DRUGBANK:DB10561" // Pork

	));

	@Override
	public void createOgerDictionaryFile(File drugbankXmlFile, File dictDirectory) throws IOException {
		File caseInsensitiveDictFile = new File(dictDirectory, "DRUGBANK.case_insensitive.tsv");
		File caseSensitiveDictFile = new File(dictDirectory, "DRUGBANK.case_sensitive.tsv");

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensWriter = FileWriterUtil.initBufferedWriter(caseInsensitiveDictFile)) {
			for (DrugbankXmlFileRecordReader rr = new DrugbankXmlFileRecordReader(drugbankXmlFile); rr.hasNext();) {
				DrugBankDrugRecord record = rr.next();

				// remove things used to test for allergies
				String description = record.getDescription();

				if (description != null
						&& (description.contains("allergenic") || description.contains("animal extract"))) {
					continue;
				}

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

				syns = augmentSynonyms(drugbankId, syns, null);
				/*
				 * split the synonyms into two sets, one that will be match in a case sensitive
				 * manner, and one that will be case insensitive
				 */
				Set<String> caseSensitiveSyns = OgerDictFileFactory.getCaseSensitiveSynonyms(syns);

				/* the synonyms set becomes the case-insensitive set */
				syns.removeAll(caseSensitiveSyns);

				for (String synonym : caseSensitiveSyns) {
					String dictLine = OgerDictFileFactory.getDictLine("DrugBank", drugbankId, synonym, drugName, "drug",
							false, null);
					OgerDictFileFactory.writeDictLine(alreadyWritten, caseSensWriter, dictLine);
				}

				for (String synonym : syns) {
					String dictLine = OgerDictFileFactory.getDictLine("DrugBank", drugbankId, synonym, drugName, "drug",
							false, null);
					OgerDictFileFactory.writeDictLine(alreadyWritten, caseInsensWriter, dictLine);
				}
			}
		}
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

//		remove all extracts
//		remove all livers
//		remove all dander
//		remove all venom but not antivenom
		// I think these should mostly, if not all, be removed by the "allergenic" test
		// above
		for (String syn : syns) {
			if (StringUtil.containsRegex(syn, "\\b[Ee]xtracts?\\b") || StringUtil.containsRegex(syn, "\\b[Vv]enom\\b")
					|| StringUtil.containsRegex(syn, "\\b[Dd]ander\\b")
					|| StringUtil.containsRegex(syn, "\\b[Aa]llergenic\\b")
					|| StringUtil.containsRegex(syn, "\\b[Ss]tomach\\b")
					|| StringUtil.containsRegex(syn, "\\b[Ll]iver\\b")) {
				toReturn = Collections.emptySet();
			}
		}

		return toReturn;
	}

}
