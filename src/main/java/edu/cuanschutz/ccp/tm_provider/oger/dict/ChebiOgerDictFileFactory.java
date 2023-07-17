package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class ChebiOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CHEBI_ROLE = "http://purl.obolibrary.org/obo/CHEBI_50906";
	private static final String CHEBI_SUBATOMIC_PARTICLE = "http://purl.obolibrary.org/obo/CHEBI_36342";
	private static final String CHEBI_ATOM = "http://purl.obolibrary.org/obo/CHEBI_33250";
	private static final String CHEBI_GROUP = "http://purl.obolibrary.org/obo/CHEBI_24433";

	public ChebiOgerDictFileFactory() {
		super("chemical", "CHEBI", SynonymSelection.EXACT_ONLY,
				Arrays.asList(CHEBI_ROLE, CHEBI_SUBATOMIC_PARTICLE, CHEBI_ATOM, CHEBI_GROUP));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		return toReturn;
	}

}
