package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class GoMfOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CELLULAR_COMPONENT = "http://purl.obolibrary.org/obo/GO_0005575";
	private static final String BIOLOGICAL_PROCESS = "http://purl.obolibrary.org/obo/GO_0008150";

	public GoMfOgerDictFileFactory() {
		super("molecular_function", "GO_MF", SynonymSelection.EXACT_ONLY,
				Arrays.asList(CELLULAR_COMPONENT, BIOLOGICAL_PROCESS));
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList());

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
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

}
