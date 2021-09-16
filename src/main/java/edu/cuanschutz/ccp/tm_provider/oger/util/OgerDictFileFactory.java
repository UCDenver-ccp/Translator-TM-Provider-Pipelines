package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugBankDrugRecord.Synonym;
import edu.ucdenver.ccp.datasource.fileparsers.drugbank.DrugbankXmlFileRecordReader;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil.SynonymType;

public class OgerDictFileFactory {

	public static void createOgerDictionaryFile(File ontologyFile, File dictFile, String ontMainType, String ontKey)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);

		int count = 0;
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				OWLClass cls = classIterator.next();
				if (count++ % 10000 == 0) {
					System.out.println("progress: " + count);
				}
				if (cls.getIRI().toString().contains(ontKey)) {
					String label = ontUtil.getLabel(cls);
					Set<String> synonyms = ontUtil.getSynonyms(cls, SynonymType.EXACT);

					if (label != null) {
						writer.write(getDictLine(ontKey, cls.getIRI().toString(), label, label, ontMainType, true));
						for (String syn : synonyms) {
							writer.write(getDictLine(ontKey, cls.getIRI().toString(), syn, label, ontMainType, true));
						}
					} else {
						System.out.println("null label id: " + cls.getIRI().toString());
					}
				}
			}
		}
	}
	
	
	public static void createOgerDictFileFromDrugbank(File drugbankXmlFile, File dictFile) throws IOException {
		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
			for (DrugbankXmlFileRecordReader rr = new DrugbankXmlFileRecordReader(drugbankXmlFile); rr.hasNext();) {
				DrugBankDrugRecord record = rr.next();

				String drugbankId = record.getDrugBankId().toString();
				String drugName = record.getDrugName();
				Set<Synonym> synonyms = record.getSynonyms();
				
				String dictLine = getDictLine("DrugBank", drugbankId, drugName, drugName, "drug", false);
				writeDictLine(alreadyWritten, writer, dictLine);

				for (Synonym synonym : synonyms) {
					dictLine = getDictLine("DrugBank", drugbankId, drugName, synonym.getSynonym(), "drug", false);
					writeDictLine(alreadyWritten, writer, dictLine);
				}
			}
		}
	}

	public static void createOgerDictFileFromHGNC(File hgncDownloadFile, File dictFile) throws IOException {

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(hgncDownloadFile, CharacterEncoding.UTF_8,
					"HGNC ID"); lineIter.hasNext();) {
				String line = lineIter.next().getText();
				String[] cols = line.split("\\t");

				// HGNC ID
				// Status
				// Approved symbol
				// Approved name
				// Alias name
				// Alias symbol

				String hgncId = cols[0];
				String approvedSymbol = cols[2];
				String approvedName = cols[3];

				String aliasName = null;
				String aliasSymbol = null;

				if (cols.length > 4) {
					aliasName = cols[4];
				}
				if (cols.length > 5) {
					aliasSymbol = cols[5];
				}

				String dictLine = getDictLine("HGNC", hgncId, approvedSymbol, approvedSymbol, "gene", false);
				writeDictLine(alreadyWritten, writer, dictLine);

				dictLine = getDictLine("HGNC", hgncId, approvedName, approvedSymbol, "gene", false);
				writeDictLine(alreadyWritten, writer, dictLine);

				if (aliasName != null) {
					dictLine = getDictLine("HGNC", hgncId, aliasName, approvedSymbol, "gene", false);
					writeDictLine(alreadyWritten, writer, dictLine);
				}

				if (aliasSymbol != null) {
					dictLine = getDictLine("HGNC", hgncId, aliasSymbol, approvedSymbol, "gene", false);
					writeDictLine(alreadyWritten, writer, dictLine);
				}

			}
		}
	}

	private static void writeDictLine(Set<String> alreadyWritten, BufferedWriter writer, String dictLine)
			throws IOException {
		if (!alreadyWritten.contains(dictLine)) {
			writer.write(dictLine);
			alreadyWritten.add(dictLine);
		}
	}

	private static String getDictLine(String ontKey, String iri, String label, String primaryLabel, String ontMainType,
			boolean processId) {
		String id = iri;
		if (processId) {
			id = StringUtil.removePrefix(iri, "http://purl.obolibrary.org/obo/");
			id = id.replace("_", ":");
			label = fixLabel(label);
			primaryLabel = fixLabel(primaryLabel);
		}

		// first column is empty (should be UMLS CUI)
		return String.format("\t%s\t%s\t%s\t%s\t%s\n", ontKey, id, label, primaryLabel, ontMainType);
	}

	static String fixLabel(String label) {
		if (label.contains("\"")) {
			label = label.substring(0, label.indexOf("\""));
		}
		if (StringUtil.endsWithRegex(label, "[(][^)]+[)]")) {
			label = StringUtil.removeSuffixRegex(label, "[(][^)]+[)]");
		}

		return label.trim();
	}

	

}
