package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class GoMfOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CELLULAR_COMPONENT = "http://purl.obolibrary.org/obo/GO_0005575";
	private static final String BIOLOGICAL_PROCESS = "http://purl.obolibrary.org/obo/GO_0008150";
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public GoMfOgerDictFileFactory() {
		super("molecular_function", "GO_MF", SynonymSelection.EXACT_ONLY,
				Arrays.asList(CELLULAR_COMPONENT, BIOLOGICAL_PROCESS));
	}

	/**
	 * This will add _MF to the GO identifiers in the generated dictionary so that
	 * they don't need to be disambiguated with CC and BP classes later on, e.g.,
	 * GO_MF:0001234
	 */
	@Override
	protected String getIdAddOn() {
		return "_MF";
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "GO_0141047", // tag
					OBO_PURL + "GO_0015267", // channel
					OBO_PURL + "GO_0048018", // signaling molecules
					OBO_PURL + "GO_0022804", // pump
					OBO_PURL + "GO_0022804", // carriers
					OBO_PURL + "GO_0022836", // gated channel
					OBO_PURL + "GO_0031386", // protein -tagged
					OBO_PURL + "GO:0005488" // binding

			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);

//		dont remove activity from: GO:0003824      enzyme

		augmentActivitySynonyms(toReturn);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	/**
	 * for synonyms that end in "activity", create a synonym that does not include
	 * "activity"
	 * 
	 * @param toReturn
	 */
	private void augmentActivitySynonyms(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.endsWith(" activity")) {
				String withoutActivity = StringUtil.removeSuffix(syn, " activity");
				System.out.println("ACTIVITY: " + withoutActivity);
				toAdd.add(withoutActivity);
			}
		}
		toReturn.addAll(toAdd);
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "GO_0008158", new HashSet<String>(Arrays.asList("patched activity")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
