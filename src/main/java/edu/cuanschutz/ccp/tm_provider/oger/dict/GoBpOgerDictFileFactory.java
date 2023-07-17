package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class GoBpOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CELLULAR_COMPONENT = "http://purl.obolibrary.org/obo/GO_0005575";
	private static final String MOLECULAR_FUNCTION = "http://purl.obolibrary.org/obo/GO_0003674";

	public GoBpOgerDictFileFactory() {
		super("biological_process", "GO_BP", SynonymSelection.EXACT_ONLY,
				Arrays.asList(MOLECULAR_FUNCTION, CELLULAR_COMPONENT));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
