package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil.SynonymType;
import edu.ucdenver.ccp.nlp.core.util.StopWordUtil;
import lombok.Data;

@Data
public abstract class OgerDictFileFactory {

	public enum SynonymSelection {
		EXACT_ONLY, EXACT_PLUS_RELATED
	}

	private final String mainType;
	private final String ontologyPrefix;
	private final SynonymSelection synSelection;
	private final List<String> classesToExclude;

	/**
	 * For an input ontology, this method creates two OGER dictionary files. One for
	 * strings that will be processed in a case-insensitive manner. The other will
	 * be used for case-sensitive search, and is targeted at acronyms an
	 * abbreviations that are in all or mostly all caps.
	 * 
	 * @param ontologyFile
	 * @param dictDirectory the directory where dictionary files will be created
	 * @para dictFileNamePrefix a prefix to be used for the generated dictionary
	 *       files
	 * @throws IOException
	 */
	public void createOgerDictionaryFile(File ontologyFile, File dictDirectory) throws IOException {
		int count = 0;

		File caseInsensitiveDictFile = new File(dictDirectory,
				String.format("%s.case_insensitive.tsv", ontologyPrefix));
		File caseSensitiveDictFile = new File(dictDirectory, String.format("%s.case_sensitive.tsv", ontologyPrefix));

		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensWriter = FileWriterUtil.initBufferedWriter(caseInsensitiveDictFile)) {

			OntologyUtil ontUtil = ontologyFile.getName().endsWith(".gz")
					? new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)))
					: new OntologyUtil(ontologyFile);
			Set<OWLClass> exclusionClasses = getExclusionClasses(ontUtil);
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				OWLClass cls = classIterator.next();
				if (count++ % 10000 == 0) {
					System.out.println("progress: " + count);
				}
				if (exclusionClasses.contains(cls)) {
					continue;
				}
				String ontologyIdPrefix = ontologyPrefix;
				if (ontologyPrefix.contains("GO_")) {
					ontologyIdPrefix = "GO";
				}
				if (cls.getIRI().toString().contains(ontologyIdPrefix)) {
					String label = ontUtil.getLabel(cls);
					if (label != null) {
						// TODO: getSynonyms gets messed up when there's an &quot; in the label -- and
						// somehow a backslash gets inserted -- the backslash breaks OGER during load.
						// For now we simply remove the backslash from the label as it doesn't happen
						// often.

						Set<String> synonyms = getSynonyms(ontUtil, cls, label, synSelection);
						synonyms = augmentSynonyms(cls.getIRI().toString(), synonyms, ontUtil);
						/*
						 * split the synonyms into two sets, one that will be match in a case sensitive
						 * manner, and one that will be case insensitive
						 */

						Set<String> caseSensitiveSyns = getCaseSensitiveSynonyms(synonyms);

						/* the synonyms set becomes the case-insensitive set */
						synonyms.removeAll(caseSensitiveSyns);

						for (String syn : caseSensitiveSyns) {
							caseSensWriter.write(getDictLine(ontologyPrefix, cls.getIRI().toString(), syn, label,
									mainType, true, getIdAddOn()));
						}
						for (String syn : synonyms) {
							caseInsensWriter.write(getDictLine(ontologyPrefix, cls.getIRI().toString(), syn, label,
									mainType, true, getIdAddOn()));
						}
					} else {
						System.out.println("null label id: " + cls.getIRI().toString());
					}
				}
			}
		} catch (OWLOntologyCreationException e) {
			throw new IOException(e);
		}
	}

	protected String getIdAddOn() {
		return null;
	}

	/**
	 * Return a set containing the label and synonyms for the specified OWLClass
	 * 
	 * @param ontUtil
	 * @param cls
	 * @param label
	 * @param synSelection
	 * @return
	 */
	public static Set<String> getSynonyms(OntologyUtil ontUtil, OWLClass cls, String label,
			SynonymSelection synSelection) {
		Set<String> synonyms = ontUtil.getSynonyms(cls, SynonymType.EXACT);
		if (synSelection == SynonymSelection.EXACT_PLUS_RELATED) {
			Set<String> relatedSyns = ontUtil.getSynonyms(cls, SynonymType.RELATED);
			synonyms.addAll(relatedSyns);
		}
		synonyms.add(label);
		synonyms = fixLabels(synonyms);
		return synonyms;
	}

	/**
	 * @param synonyms
	 * @return a set containing the synonyms that should be match in a
	 *         case-sensitive manner
	 */
	public static Set<String> getCaseSensitiveSynonyms(Set<String> synonyms) {
		Set<String> caseSensitiveSyns = new HashSet<String>();
		for (String syn : synonyms) {
			if (isCaseSensitive(syn)) {
				caseSensitiveSyns.add(syn);

				// if the synonym is all uppercase, then add a version where the first letter is
				// uppercase by the other letters are lowercase, e.g. Brca1 or Rad51
				if (syn.matches("^[A-Z]+[0-9]+")) {
					StringBuilder alternate = new StringBuilder();
					for (Character c : syn.toCharArray()) {
						if (alternate.length() == 0) {
							alternate.append(Character.toUpperCase(c));
						} else if (Character.isAlphabetic(c)) {
							alternate.append(Character.toLowerCase(c));
						} else {
							alternate.append(c);
						}
					}
					caseSensitiveSyns.add(alternate.toString());
				}

			}
		}

		return caseSensitiveSyns;
	}

	/**
	 * Return any string that is > 40% uppercase, or starts with number, or has a
	 * mix of uppercase and lowercase letters, but the uppercase letters aren't just
	 * the first letters of words
	 * 
	 * @param syn
	 * @return
	 */
	protected static boolean isCaseSensitive(String s) {

		// if it starts with a number, consider it case-sensitive
		if (s.matches("^\\d")) {
			return true;
		}

		// if there are more than 40% uppercase+digits, consider it case sensitive
		String sTrimmed = s.trim();
		int ucCount = 0;
		for (int i = 0; i < sTrimmed.length(); i++) {
			char c = sTrimmed.charAt(i);
			if (Character.isUpperCase(c) || Character.isDigit(c)) {
				ucCount++;
			}

		}
		float percentUpper = (float) ucCount / (float) sTrimmed.length();
		if (percentUpper > 0.4) {
			return true;
		}

		// if there are a mix of upper and lowercase letters and the uppercase letters
		// aren't just the first letter of the word, then consider case-sensitive, e.g.
		// casE
		Pattern p = Pattern.compile("[a-z][A-Z]");
		Matcher m = p.matcher(s);
		if (m.find()) {
			return true;
		}

		// if there are multiple tokens and one of the tokens meets the above criteria
		// for case sentitivity, then consider the entire label to be case sensitive.
		String[] tokens = s.split(" ");
		if (tokens.length > 1) {
			for (String token : tokens) {
				if (isCaseSensitive(token)) {
					return true;
				}
			}
		}

		return false;

	}

	/**
	 * removes any stopwords in the input set
	 * 
	 * @param input
	 * @return
	 */
	protected Set<String> removeStopWords(Set<String> input) {
		Set<String> updatedSet = new HashSet<String>();
		Set<String> stopwords = new HashSet<String>(StopWordUtil.STOPWORDS);
		for (String syn : input) {
			if (!stopwords.contains(syn.toLowerCase())) {
				updatedSet.add(syn);
			}
		}
		return updatedSet;
	}

	/**
	 * remove words from the input set that are less than length l (in character
	 * count)
	 * 
	 * @param input
	 * @param l
	 * @return
	 */
	protected Set<String> removeWordsLessThenLength(Set<String> input, int l) {
		Set<String> updatedSet = new HashSet<String>();
		for (String syn : input) {
			if (syn.length() >= l) {
				updatedSet.add(syn);
			}
		}
		return updatedSet;
	}

	/**
	 * Return the OWLClass objects for the specified classesToExclude including all
	 * descendents
	 * 
	 * @param ontUtil
	 * @return
	 */
	private Set<OWLClass> getExclusionClasses(OntologyUtil ontUtil) {
		Set<OWLClass> clses = new HashSet<OWLClass>();
		if (classesToExclude != null) {
			for (String clsName : classesToExclude) {
				OWLClass cls = ontUtil.getOWLClassFromId(clsName);
				clses.add(cls);
				clses.addAll(ontUtil.getDescendents(cls));
				if (cls == null) {
					throw new IllegalArgumentException(
							String.format("Unable to find exlusion class (%s) in the ontology.", clsName));
				}
			}
		}
		return clses;
	}

	protected abstract Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil);

//	protected abstract Set<String> filterSynonyms(String iri, Set<String> syns);

//	public static void createOgerDictionaryFile_MONDO(File ontologyFile, File dictFile, String ontMainType,
//			String ontKey) throws OWLOntologyCreationException, FileNotFoundException, IOException {
//		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);
//
//		OWLClass diseaseCharacteristic = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0021125");
//		OWLClass defectCls = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0008568");
//		OWLClass thyroidTumorCls = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/MONDO_0015074");
//
//		int count = 0;
////		List<String> singleTokenRelatedSynonyms = new ArrayList<String>();
//		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
//			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
//				OWLClass cls = classIterator.next();
//				if (count++ % 10000 == 0) {
//					System.out.println("progress: " + count);
//				}
//				Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
//				if (ancestors.contains(diseaseCharacteristic) || cls.equals(diseaseCharacteristic)) {
//					// exclude all descendents of disease_characteristic
//					continue;
//				}
//
//				if (cls.getIRI().toString().contains(ontKey)) {
//					String label = ontUtil.getLabel(cls);
//					Set<String> synonyms = ontUtil.getSynonyms(cls, SynonymType.EXACT);
//					Set<String> relatedSyns = ontUtil.getSynonyms(cls, SynonymType.RELATED);
//					synonyms.addAll(relatedSyns);
//					synonyms.add(label);
//					synonyms = augmentSynonyms(synonyms);
//
////					for (String syn : relatedSyns) {
////						if (!syn.contains(" ")) {
////							singleTokenRelatedSynonyms.add(syn);
////						}
////					}
//
//					if (cls.equals(defectCls)) {
//						synonyms.remove("defect");
//					}
//
//					if (cls.equals(thyroidTumorCls)) {
//						synonyms.remove("THYROID");
//					}
//
//					if (label != null) {
//						writer.write(getDictLine(ontKey, cls.getIRI().toString(), label, label, ontMainType, true));
//						for (String syn : synonyms) {
//							writer.write(getDictLine(ontKey, cls.getIRI().toString(), syn, label, ontMainType, true));
//						}
//					} else {
//						System.out.println("null label id: " + cls.getIRI().toString());
//					}
//				}
//			}
//		}
//
////		Collections.sort(singleTokenRelatedSynonyms);
////		try (BufferedWriter writer = FileWriterUtil
////				.initBufferedWriter(new File(dictFile.getParentFile(), "mondo_single_token_related_synonyms.txt"))) {
////			for (String syn : singleTokenRelatedSynonyms) {
////				writer.write(syn + "\n");
////			}
////		}
//
//	}
//
//	private static Set<String> augmentSynonyms(Set<String> synonyms) {
//
//		Set<String> toReturn = new HashSet<String>(synonyms);
//
//		// remove stopwords
//		Set<String> stopwords = new HashSet<String>(StopWordUtil.STOPWORDS);
//		Set<String> toRemove = new HashSet<String>();
//		for (String syn : toReturn) {
//			if (stopwords.contains(syn.toLowerCase())) {
//				toRemove.add(syn);
//			}
//		}
//		toReturn.removeAll(toRemove);
//
//		// for all classes that are "... virus infection" -- add a synonym that is just
//		// "... virus"
//		Set<String> toAdd = new HashSet<String>();
//		for (String syn : toReturn) {
//			if (syn.endsWith("virus infection")) {
//				String virus = StringUtil.removeSuffix(syn, " infection");
//				toAdd.add(virus);
//			} else if (syn.endsWith("virus infections")) {
//				String virus = StringUtil.removeSuffix(syn, " infections");
//				toAdd.add(virus);
//			}
//		}
//		toReturn.addAll(toAdd);
//
//		// remove ", formerly" or "(formerly)"
//		toAdd = new HashSet<String>();
//		for (String syn : toReturn) {
//			if (syn.contains(", formerly")) {
//				toAdd.add(syn.replace(", formerly", ""));
//			} else if (syn.contains("(formerly)")) {
//				toAdd.add(syn.replace("(formerly)", ""));
//			}
//		}
//		toReturn.addAll(toAdd);
//
//		// remove single character synonyms
//		toRemove = new HashSet<String>();
//		for (String syn : toReturn) {
//			if (syn.length() == 1) {
//				toRemove.add(syn);
//			}
//		}
//		toReturn.removeAll(toRemove);
//
//		return toReturn;
//	}

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

				String dictLine = getDictLine("HGNC", hgncId, approvedSymbol, approvedSymbol, "gene", false, null);
				writeDictLine(alreadyWritten, writer, dictLine);

				dictLine = getDictLine("HGNC", hgncId, approvedName, approvedSymbol, "gene", false, null);
				writeDictLine(alreadyWritten, writer, dictLine);

				if (aliasName != null) {
					dictLine = getDictLine("HGNC", hgncId, aliasName, approvedSymbol, "gene", false, null);
					writeDictLine(alreadyWritten, writer, dictLine);
				}

				if (aliasSymbol != null) {
					dictLine = getDictLine("HGNC", hgncId, aliasSymbol, approvedSymbol, "gene", false, null);
					writeDictLine(alreadyWritten, writer, dictLine);
				}

			}
		}
	}

	/**
	 * @param moleproChemicalLabelFile - file provided by MolePro that contains
	 *                                 labels mapped to pubchem identifiers
	 * @param dictFile
	 * @throws IOException
	 */
	public static void createChemicalOgerDictFile(File moleproChemicalLabelFile, File dictFile) throws IOException {

		String id = null;
		Set<String> labels = new HashSet<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(dictFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(moleproChemicalLabelFile, CharacterEncoding.UTF_8,
					null); lineIter.hasNext();) {
				String line = lineIter.next().getText();
				String[] cols = line.split("\\t");

				String pubchemId = cols[0];
				String label = processChemicalLabel(pubchemId, cols[1]);

				if (label != null) {
					if (id == null || id.equals(pubchemId)) {
						id = pubchemId;
						labels.add(label);
					} else {
						// we've reached a new pubchem ID so it's time to serialize the labels for the
						// previous ID and then reset the id and set
						for (String lbl : labels) {
							String dictLine = getDictLine("PUBCHEM", id, lbl, lbl, "chemical", false, null);
							writeDictLine(new HashSet<String>(), writer, dictLine);
						}

						id = pubchemId;
						labels = new HashSet<String>();
						labels.add(label);
					}
				}
			}

			// write the final set of labels
			for (String lbl : labels) {
				String dictLine = getDictLine("PUBCHEM", id, lbl, lbl, "chemical", false, null);
				writeDictLine(new HashSet<String>(), writer, dictLine);
			}
		}
	}

	private static String processChemicalLabel(String id, String s) {
		// if < 4 characters after removing punctuation, then exclude
		String sNoPunct = s.replaceAll("\\p{Punct}", "");
		if (sNoPunct.length() < 4) {
//			System.out.println("Excluding: "+ s);
			return null;
		}

		// if there's only one comma, then move the right side to be in front of the
		// left side
		String[] cols = s.split(",");
		if (cols.length == 2) {
			String l = cols[1] + (cols[1].endsWith("-") ? "" : " ") + cols[0];
//			System.out.println("Flipping: " + cols[1] + " | " + cols[0] + " = " + l);
			return l;
		} else if (cols.length > 2) {
			// exclude labels with more than one comma
//			System.out.println("Excluding -- too many commas: " + s);
			return null;
		}

		// remove labels that are > 30% digits
		String noDigits = s.replaceAll("\\d", "").replaceAll("\\s", "");
		float percentDigits = (float) (s.length() - noDigits.length()) / (float) s.length();
		if (percentDigits > 0.3) {
//			System.out.println("Exclude based on percent digits: " + s);
			return null;
		}

		// if the label is surrounded by square brackets, remove them
		if (s.startsWith("[") && s.endsWith("]")) {
			return s.substring(1, s.length() - 1);
		}

		// specific exclusions
		if (id.equalsIgnoreCase("PUBCHEM.COMPOUND:444212") && s.equalsIgnoreCase("Acid")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:139199449") && s.equalsIgnoreCase("ligand")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:4201") && s.equalsIgnoreCase("solution")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:3036828") && s.equalsIgnoreCase("methyl")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:135616186") && s.equalsIgnoreCase("focus")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:135438605") && s.equalsIgnoreCase("focus")) {
			return null;
		}
		if (id.equalsIgnoreCase("pubchem.compound:4641") && s.equalsIgnoreCase("optimal")) {
			return null;
		}

		return s;
	}

	public static void writeDictLine(Set<String> alreadyWritten, BufferedWriter writer, String dictLine)
			throws IOException {
		if (!alreadyWritten.contains(dictLine)) {
			writer.write(dictLine);
			alreadyWritten.add(dictLine);
		}
	}

	/**
	 * Ensure there are 6 columns
	 * 
	 * @param line
	 */
	public static void validateDictLine(String line) {
		String[] cols = line.split("\t");
		if (cols.length != 6) {
			throw new IllegalArgumentException(String.format("Unexpected column count (%d) on line: %s", cols.length,
					line.replaceAll("\\t", "[TAB]")));
		}
	}

	/**
	 * @param ontKey
	 * @param iri
	 * @param label
	 * @param primaryLabel
	 * @param ontMainType
	 * @param processId
	 * @param idAddOn      used to add _BP, _CC, _MF to GO concepts so we don't have
	 *                     to disambiguatae them later
	 * @return
	 */
	public static String getDictLine(String ontKey, String iri, String label, String primaryLabel, String ontMainType,
			boolean processId, String idAddOn) {
		String id = iri;
		if (processId) {
			id = StringUtil.removePrefix(iri, "http://purl.obolibrary.org/obo/");
			id = id.replace("_", ":");
			if (idAddOn != null) {
				String idPrefix = id.split(":")[0];
				String idSuffix = id.split(":")[1];
				id = String.format("%s%s:%s", idPrefix, idAddOn, idSuffix);
			}
			label = fixLabel(label);
			primaryLabel = fixLabel(primaryLabel);
		}

		// first column is empty (should be UMLS CUI)
		String line = String.format("\t%s\t%s\t%s\t%s\t%s\n", ontKey.trim(), id.trim(), label.trim(),
				primaryLabel.trim(), ontMainType.trim());
		validateDictLine(line);
		return line;
	}

	protected static String fixLabel(String label) {
		// replace \" with "
//		System.out.println("0: " + label);
		label = label.replaceAll("\\\\\"", "\"");

//		System.out.println("1: " + label);

		// if there is an odd number of quotes, and there is one at the end of the
		// label, then remove it
		if (label.split("\"").length % 2 == 1 && label.endsWith("\"")) {
			label = StringUtil.removeSuffix(label, "\"");
		}
//		System.out.println("2: " + label);

		// remove parentheticals when they occur at the end of the label and there is a
		// space between label text and the parenthetical
		if (StringUtil.endsWithRegex(label, " [(][^)]+[)]")) {
			label = StringUtil.removeSuffixRegex(label, " [(][^)]+[)]");
		}

//		System.out.println("3: " + label);
		return label.trim();
	}

	protected static Set<String> fixLabels(Set<String> labels) {
		Set<String> fixed = new HashSet<String>();
		for (String l : labels) {
			fixed.add(fixLabel(l));
		}
		return fixed;
	}

}
