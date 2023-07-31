package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class GoMfOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CELLULAR_COMPONENT = "http://purl.obolibrary.org/obo/GO_0005575";
	private static final String BIOLOGICAL_PROCESS = "http://purl.obolibrary.org/obo/GO_0008150";

	public GoMfOgerDictFileFactory() {
		super("molecular_function", "GO_MF", SynonymSelection.EXACT_ONLY,
				Arrays.asList(CELLULAR_COMPONENT, BIOLOGICAL_PROCESS));
	}

	private static final Set<String> IRIS_TO_EXCLUDE = new HashSet<String>(Arrays.asList(
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			"", //
			""));
	
	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		
		if (IRIS_TO_EXCLUDE.contains(iri)) {
			toReturn = Collections.emptySet();
		}
		
		return toReturn;
	}

}
