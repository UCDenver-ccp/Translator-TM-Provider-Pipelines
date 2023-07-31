package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class SoOgerDictFileFactory extends OgerDictFileFactory {

	public SoOgerDictFileFactory() {
		super("sequence_feature", "SO", SynonymSelection.EXACT_ONLY, null);
	}

	private static final Set<String> IRIS_TO_EXCLUDE = new HashSet<String>(Arrays.asList());

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
