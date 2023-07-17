package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class HpOgerDictFileFactory extends OgerDictFileFactory {

	public HpOgerDictFileFactory() {
		super("phenotype", "HP", SynonymSelection.EXACT_ONLY, null);
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
