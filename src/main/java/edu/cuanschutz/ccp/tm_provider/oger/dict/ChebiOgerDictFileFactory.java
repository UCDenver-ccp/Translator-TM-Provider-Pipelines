package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class ChebiOgerDictFileFactory extends OgerDictFileFactory {

	
	private static final String CHEBI_ROLE = "http://purl.obolibrary.org/obo/CHEBI_50906";
	private static final String CHEBI_SUBATOMIC_PARTICLE = "http://purl.obolibrary.org/obo/CHEBI_36342";
	private static final String CHEBI_ATOM = "http://purl.obolibrary.org/obo/CHEBI_33250";
	private static final String CHEBI_GROUP = "http://purl.obolibrary.org/obo/CHEBI_24433";
	public static List<String> EXCLUDED_CLASSES = Arrays.asList(CHEBI_ROLE, CHEBI_SUBATOMIC_PARTICLE, CHEBI_ATOM, CHEBI_GROUP);

	public ChebiOgerDictFileFactory() {
		super("chemical", "CHEBI", SynonymSelection.EXACT_ONLY,
				Arrays.asList(CHEBI_ROLE, CHEBI_SUBATOMIC_PARTICLE, CHEBI_ATOM, CHEBI_GROUP));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 2);
		toReturn = filterSynonyms(toReturn);
		return toReturn;
	}

	private Set<String> filterSynonyms(Set<String> synonyms) {
		// remove any synonym with 4 or more hyphens or 3 or more commas -- this will exclude complicated
		// chemical names that are unlikely to match anyway
		Set<String> filtered = new HashSet<String>();
		for (String syn : synonyms) {
			String[] hyphenToks = syn.split("-");
			String[] commaToks = syn.split(",");
			if (hyphenToks.length < 5 && commaToks.length < 4) {
				filtered.add(syn);
			}
		}

		return filtered;
	}

}
