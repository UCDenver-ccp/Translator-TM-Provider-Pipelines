package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class SoOgerDictFileFactory extends OgerDictFileFactory {

	public SoOgerDictFileFactory() {
		super("sequence_feature", "SO", SynonymSelection.EXACT_ONLY, null);
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
