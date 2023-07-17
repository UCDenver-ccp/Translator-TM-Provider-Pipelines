package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;

public class MondoOgerDictFileFactory extends OgerDictFileFactory {

	private static final String DISEASE_CHARACTERISTIC = "http://purl.obolibrary.org/obo/MONDO_0021125";
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public MondoOgerDictFileFactory() {
		super("disease", "MONDO", SynonymSelection.EXACT_ONLY, Arrays.asList(DISEASE_CHARACTERISTIC));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);

		augmentVirusSynonyms(toReturn);
		augmentSynonymsWithFormerly(toReturn);
		// remove single character synonyms
		toReturn = removeWordsLessThenLength(toReturn, 2);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		return toReturn;
	}

	/**
	 * For synonyms that have "formerly" in them, add a synonym where "formerly" is
	 * removed
	 * 
	 * @param toReturn
	 */
	private void augmentSynonymsWithFormerly(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
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
	}

	/**
	 * for all classes that are "X virus infection" -- add a synonym that is just "X
	 * virus"
	 * 
	 * @param toReturn
	 */
	private void augmentVirusSynonyms(Set<String> toReturn) {
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
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "MONDO_0005059", new HashSet<String>(Arrays.asList("blood")));
		map.put(OBO_PURL + "MONDO_0008568", new HashSet<String>(Arrays.asList("defect")));
		map.put(OBO_PURL + "MONDO_0015074", new HashSet<String>(Arrays.asList("THYROID")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
