package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class HpOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CLINICAL_MODIFIER_IRI = "http://purl.obolibrary.org/obo/HP_0012823";
	private static final String FREQUENCY_IRI = "http://purl.obolibrary.org/obo/HP_0040279";
	private static final String PAST_MEDICAL_HISTORY_IRI = "http://purl.obolibrary.org/obo/HP_0032443";
	private static final String MODE_OF_INHERITANCE_IRI = "http://purl.obolibrary.org/obo/HP_0000005";
	private static final String BLOOD_GROUP_IRI = "http://purl.obolibrary.org/obo/HP_0032223";

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public HpOgerDictFileFactory() {
		super("phenotype", "HP", SynonymSelection.EXACT_ONLY, Arrays.asList(CLINICAL_MODIFIER_IRI, FREQUENCY_IRI,
				PAST_MEDICAL_HISTORY_IRI, MODE_OF_INHERITANCE_IRI, BLOOD_GROUP_IRI));
	}

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "HP_0001548" // overgrowth

			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "HP_0030212", new HashSet<String>(Arrays.asList("Collecting")));
		map.put(OBO_PURL + "HP_0000733", new HashSet<String>(Arrays.asList("Stereotyped")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
