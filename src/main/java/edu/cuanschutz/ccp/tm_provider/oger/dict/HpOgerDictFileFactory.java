package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class HpOgerDictFileFactory extends OgerDictFileFactory {

	private static final String CLINICAL_MODIFIER_IRI = "http://purl.obolibrary.org/obo/HP_0012823";
	private static final String FREQUENCY_IRI = "http://purl.obolibrary.org/obo/HP_0040279";
	private static final String PAST_MEDICAL_HISTORY_IRI = "http://purl.obolibrary.org/obo/HP_0032443";
	private static final String MODE_OF_INHERITANCE_IRI = "http://purl.obolibrary.org/obo/HP_0000005";
	private static final String BLOOD_GROUP_IRI = "http://purl.obolibrary.org/obo/HP_0032223";

	public HpOgerDictFileFactory() {
		super("phenotype", "HP", SynonymSelection.EXACT_ONLY, Arrays.asList(CLINICAL_MODIFIER_IRI, FREQUENCY_IRI,
				PAST_MEDICAL_HISTORY_IRI, MODE_OF_INHERITANCE_IRI, BLOOD_GROUP_IRI));
	}

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList());

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

}
