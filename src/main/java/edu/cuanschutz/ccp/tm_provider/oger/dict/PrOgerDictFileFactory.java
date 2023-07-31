package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class PrOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public PrOgerDictFileFactory() {
		super("protein", "PR", SynonymSelection.EXACT_PLUS_RELATED, null);
	}

	private static final Set<String> IRIS_TO_EXCLUDE = new HashSet<String>(Arrays.asList(OBO_PURL + "PR_000000001", // protein
			OBO_PURL + "PR_000003507" // chimeric protein
	));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);

		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		if (IRIS_TO_EXCLUDE.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "PR_000005054", new HashSet<String>(Arrays.asList("MICE")));
		map.put(OBO_PURL + "PR_P19576", new HashSet<String>(Arrays.asList("male")));
		map.put(OBO_PURL + "PR_000034898", new HashSet<String>(Arrays.asList("year")));
		map.put(OBO_PURL + "PR_000002226", new HashSet<String>(Arrays.asList("see")));
		map.put(OBO_PURL + "PR_P33696", new HashSet<String>(Arrays.asList("exon")));
		map.put(OBO_PURL + "PR_P52558", new HashSet<String>(Arrays.asList("pure")));
		map.put(OBO_PURL + "PR_000023162", new HashSet<String>(Arrays.asList("many")));
		map.put(OBO_PURL + "PR_000022679", new HashSet<String>(Arrays.asList("fold")));
		map.put(OBO_PURL + "PR_000023552", new HashSet<String>(Arrays.asList("pine")));
		map.put(OBO_PURL + "PR_000023270", new HashSet<String>(Arrays.asList("mode")));
		map.put(OBO_PURL + "PR_Q8N1N2", new HashSet<String>(Arrays.asList("full")));
		map.put(OBO_PURL + "PR_000034142", new HashSet<String>(Arrays.asList("case")));
		map.put(OBO_PURL + "PR_Q8N1N2", new HashSet<String>(Arrays.asList("full")));
		map.put(OBO_PURL + "PR_P02301", new HashSet<String>(Arrays.asList("embryonic")));
		map.put(OBO_PURL + "PR_000023165", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P19994", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P0A078", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_000023786", new HashSet<String>(Arrays.asList("RNA")));
		map.put(OBO_PURL + "PR_Q99856", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_Q62431", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_000008536", new HashSet<String>(Arrays.asList("hrs")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
