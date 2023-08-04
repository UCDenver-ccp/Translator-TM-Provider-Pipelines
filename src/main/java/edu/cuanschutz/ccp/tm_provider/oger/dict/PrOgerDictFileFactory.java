package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class PrOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private final Set<String> englishWords;
	private BufferedWriter tmpWriter;
	private final Map<String, Set<String>> iriToExcludedEnglishWordLabels;

	/**
	 * @param englishWordFile
	 * @param englishWordMatchLogFile
	 * @param excludeEnglishWordFile  - this file has been manually curated to
	 *                                exclude labels that are likely false positives
	 * @throws IOException
	 */
	public PrOgerDictFileFactory(File englishWordFile, File englishWordMatchLogFile, File excludeEnglishWordFile)
			throws IOException {
		super("protein", "PR", SynonymSelection.EXACT_PLUS_RELATED, null);
		System.out.print("Loading English words...");
		List<String> words = FileReaderUtil.loadLinesFromFile(englishWordFile, CharacterEncoding.UTF_8);
		englishWords = new HashSet<String>(words);
		System.out.println("complete.");
		iriToExcludedEnglishWordLabels = loadExcludedEnglishWordLabels(excludeEnglishWordFile);

		tmpWriter = FileWriterUtil.initBufferedWriter(englishWordMatchLogFile);
	}

	/**
	 * Loads a map from IRI to English word labels that should be excluded (from a
	 * manually curated file)
	 * 
	 * @param excludeEnglishWordFile
	 * @return
	 * @throws IOException
	 */
	private Map<String, Set<String>> loadExcludedEnglishWordLabels(File excludeEnglishWordFile) throws IOException {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		for (StreamLineIterator lineIter = new StreamLineIterator(excludeEnglishWordFile,
				CharacterEncoding.UTF_8); lineIter.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String excludeFlag = cols[0];
//			String lowercaseLabel = cols[1];
			String label = cols[2];
			String iri = OBO_PURL + cols[3];

			if (excludeFlag.contains("x")) {
				CollectionsUtil.addToOne2ManyUniqueMap(iri, label, map);
			}
		}
		return map;
	}

	public void close() throws IOException {
		tmpWriter.close();
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "PR_000000001", // protein
					OBO_PURL + "PR_000003507" // chimeric protein
			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);

		toReturn = removeWordsLessThenLength(toReturn, 4);
		toReturn = filterSpecificSynonyms(iri, toReturn);
		// log matches to english words for further manual curation
		logEnglishWords(iri, toReturn);
		// remove matches to english words that have been manually indicated for
		// exclusion
		toReturn = filterEnglishWordLabels(iri, toReturn);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	private Set<String> filterEnglishWordLabels(String iri, Set<String> synonyms) {
		Set<String> toReturn = new HashSet<String>(synonyms);
		if (iriToExcludedEnglishWordLabels.containsKey(iri)) {
			System.out.println("Exclusing: " + iriToExcludedEnglishWordLabels.get(iri).toString());
			toReturn.removeAll(iriToExcludedEnglishWordLabels.get(iri));
		}
		return toReturn;
	}

	private void logEnglishWords(String iri, Set<String> synonyms) {
		for (String syn : synonyms) {
			if (syn.length() > 3) {
				if (englishWords.contains(syn.toLowerCase())) {
//				System.out.println(String.format("WORD Possible bad label: %s -- %s", iri, syn));
					try {
						tmpWriter.write(String.format("%s\t%s\n", iri.toString(), syn));
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(-1);
					}
					// TODO - are we going to do this automatically, or curate a list?
					// toReturn.remove(syn);
				}
			}
		}
	}

	/**
	 * This manual list was formed prior to the comprehensive check against English words
	 * @param iri
	 * @param syns
	 * @return
	 */
	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "PR_000005054", new HashSet<String>(Arrays.asList("MICE")));
		map.put(OBO_PURL + "PR_P19576", new HashSet<String>(Arrays.asList("malE")));
		map.put(OBO_PURL + "PR_000034898", new HashSet<String>(Arrays.asList("yeaR")));
		map.put(OBO_PURL + "PR_000002226", new HashSet<String>(Arrays.asList("see")));
		map.put(OBO_PURL + "PR_P33696", new HashSet<String>(Arrays.asList("exoN")));
		map.put(OBO_PURL + "PR_P52558", new HashSet<String>(Arrays.asList("purE")));
		map.put(OBO_PURL + "PR_000023162", new HashSet<String>(Arrays.asList("manY")));
		map.put(OBO_PURL + "PR_000022679", new HashSet<String>(Arrays.asList("folD")));
		map.put(OBO_PURL + "PR_000023552", new HashSet<String>(Arrays.asList("pinE", "pin")));
		map.put(OBO_PURL + "PR_000023270", new HashSet<String>(Arrays.asList("modE")));
		map.put(OBO_PURL + "PR_Q8N1N2", new HashSet<String>(Arrays.asList("Full")));
		map.put(OBO_PURL + "PR_000034142", new HashSet<String>(Arrays.asList("casE")));
		map.put(OBO_PURL + "PR_P02301", new HashSet<String>(Arrays.asList("embryonic")));
		map.put(OBO_PURL + "PR_000023165", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P19994", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P0A078", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_000023786", new HashSet<String>(Arrays.asList("rna")));
		map.put(OBO_PURL + "PR_Q99856", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_Q62431", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_000008536", new HashSet<String>(Arrays.asList("Hrs")));
		map.put(OBO_PURL + "PR_Q9VAH4", new HashSet<String>(Arrays.asList("fig")));
		map.put(OBO_PURL + "PR_000016181", new HashSet<String>(Arrays.asList("Out")));
		map.put(OBO_PURL + "PR_000014717", new HashSet<String>(Arrays.asList("SET")));
		map.put(OBO_PURL + "PR_Q9VVR1", new HashSet<String>(Arrays.asList("not")));
		map.put(OBO_PURL + "PR_000029532", new HashSet<String>(Arrays.asList("embryonic")));
		map.put(OBO_PURL + "PR_000023516", new HashSet<String>(Arrays.asList("act")));
		map.put(OBO_PURL + "PR_000009667", new HashSet<String>(Arrays.asList("LARGE")));
		map.put(OBO_PURL + "PR_000002222", new HashSet<String>(Arrays.asList("LIGHT")));
		map.put(OBO_PURL + "PR_Q8INV7", new HashSet<String>(Arrays.asList("Victoria")));
		map.put(OBO_PURL + "PR_Q9CQS6", new HashSet<String>(Arrays.asList("Blot")));
		map.put(OBO_PURL + "PR_Q29TV8", new HashSet<String>(Arrays.asList("Blot")));
		map.put(OBO_PURL + "PR_Q62813", new HashSet<String>(Arrays.asList("Lamp")));
		map.put(OBO_PURL + "PR_000001067", new HashSet<String>(Arrays.asList("Alpha")));
		map.put(OBO_PURL + "PR_Q07342", new HashSet<String>(Arrays.asList("axial")));
		map.put(OBO_PURL + "PR_Q9V853", new HashSet<String>(Arrays.asList("lack")));
		map.put(OBO_PURL + "PR_Q9WVF7", new HashSet<String>(Arrays.asList("Pole")));
		map.put(OBO_PURL + "PR_Q54RD4", new HashSet<String>(Arrays.asList("pole")));
		map.put(OBO_PURL + "PR_Q7KRY6", new HashSet<String>(Arrays.asList("ball")));
		map.put(OBO_PURL + "PR_Q7TSD4", new HashSet<String>(Arrays.asList("Spatial")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
