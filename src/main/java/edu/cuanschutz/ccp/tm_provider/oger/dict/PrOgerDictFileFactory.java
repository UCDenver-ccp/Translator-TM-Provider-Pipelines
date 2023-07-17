package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class PrOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public PrOgerDictFileFactory() {
		super("protein", "PR", SynonymSelection.EXACT_ONLY, null);
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);

		toReturn = removeWordsLessThenLength(toReturn, 2);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "PR_000005054", new HashSet<String>(Arrays.asList("MICE")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
