package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class GoCcOgerDictFileFactory extends OgerDictFileFactory {

	private static final String MOLECULAR_FUNCTION = "http://purl.obolibrary.org/obo/GO_0003674";
	private static final String BIOLOGICAL_PROCESS = "http://purl.obolibrary.org/obo/GO_0008150";

	public GoCcOgerDictFileFactory() {
		super("cellular_component", "GO_CC", SynonymSelection.EXACT_ONLY,
				Arrays.asList(MOLECULAR_FUNCTION, BIOLOGICAL_PROCESS));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
