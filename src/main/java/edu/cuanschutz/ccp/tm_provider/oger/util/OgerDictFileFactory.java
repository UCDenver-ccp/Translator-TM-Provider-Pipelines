package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil.SynonymType;
import edu.ucdenver.ccp.nlp.core.util.StopWordUtil;

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

	public static void createOgerDictionaryFile_MONDO(File ontologyFile, File dictFile, String ontMainType,
			String ontKey) throws OWLOntologyCreationException, FileNotFoundException, IOException {
		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);

		OWLClass diseaseCharacteristic = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0021125");
		OWLClass defectCls = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0008568");
		OWLClass thyroidTumorCls = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0015074");
		
		int count = 0;
		List<String> singleTokenRelatedSynonyms = new ArrayList<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				OWLClass cls = classIterator.next();
				if (count++ % 10000 == 0) {
					System.out.println("progress: " + count);
				}
				Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
				if (ancestors.contains(diseaseCharacteristic) || cls.equals(diseaseCharacteristic)) {
					// exclude all descendents of disease_characteristic
					continue;
				}

				if (cls.getIRI().toString().contains(ontKey)) {
					String label = ontUtil.getLabel(cls);
					Set<String> synonyms = ontUtil.getSynonyms(cls, SynonymType.EXACT);
					Set<String> relatedSyns = ontUtil.getSynonyms(cls, SynonymType.RELATED);
					synonyms.addAll(relatedSyns);
					synonyms.add(label);
					synonyms = augmentSynonyms(synonyms);

					for (String syn : relatedSyns) {
						if (!syn.contains(" ")) {
							singleTokenRelatedSynonyms.add(syn);
						}
					}

					if (cls.equals(defectCls)) {
						synonyms.remove("defect");
					}
					
					if (cls.equals(thyroidTumorCls)) {
						synonyms.remove("THYROID");
					}

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

		Collections.sort(singleTokenRelatedSynonyms);
		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new File(dictFile.getParentFile(), "mondo_single_token_related_synonyms.txt"))) {
			for (String syn : singleTokenRelatedSynonyms) {
				writer.write(syn + "\n");
			}
		}

	}

	private static Set<String> augmentSynonyms(Set<String> synonyms) {

		Set<String> toReturn = new HashSet<String>(synonyms);

		// remove stopwords
		Set<String> stopwords = new HashSet<String>(StopWordUtil.STOPWORDS);
		Set<String> toRemove = new HashSet<String>();
		for (String syn : toReturn) {
			if (stopwords.contains(syn.toLowerCase())) {
				toRemove.add(syn);
			}
		}
		toReturn.removeAll(toRemove);

		// for all classes that are "... virus infection" -- add a synonym that is just
		// "... virus"
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.endsWith("virus infection")) {
				String virus = StringUtil.removeSuffix(syn, " infection");
				toAdd.add(virus);
			} else if (syn.endsWith("virus infections")) {
				String virus = StringUtil.removeSuffix(syn, " infections");
				toAdd.add(virus);
			}
		}
		toReturn.addAll(toAdd);

		// remove ", formerly" or "(formerly)"
		toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.contains(", formerly")) {
				System.out.println("Adding: " + syn.replace(", formerly", ""));
				toAdd.add(syn.replace(", formerly", ""));
			} else if (syn.contains("(formerly)")) {
				System.out.println("Adding: " + syn.replace("(formerly)", ""));
				toAdd.add(syn.replace("(formerly)", ""));
			}
		}
		toReturn.addAll(toAdd);

		// remove single character synonyms
		toRemove = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.length() == 1) {
				toRemove.add(syn);
			}
		}
		toReturn.removeAll(toRemove);

		return toReturn;
	}

	// this block commented as it depends on an update to the datasource fileparser
	// library to make the Synonym class visible.
//	public static void createOgerDictFileFromDrugbank(File drugbankXmlFile, File dictFile) throws IOException {
//		Set<String> alreadyWritten = new HashSet<String>();
//		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
//			for (DrugbankXmlFileRecordReader rr = new DrugbankXmlFileRecordReader(drugbankXmlFile); rr.hasNext();) {
//				DrugBankDrugRecord record = rr.next();
//
//				String drugbankId = record.getDrugBankId().toString();
//				String drugName = record.getDrugName();
//				Set<Synonym> synonyms = record.getSynonyms();
//				
//				String dictLine = getDictLine("DrugBank", drugbankId, drugName, drugName, "drug", false);
//				writeDictLine(alreadyWritten, writer, dictLine);
//
//				for (Synonym synonym : synonyms) {
//					dictLine = getDictLine("DrugBank", drugbankId, drugName, synonym.getSynonym(), "drug", false);
//					writeDictLine(alreadyWritten, writer, dictLine);
//				}
//			}
//		}
//	}

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
