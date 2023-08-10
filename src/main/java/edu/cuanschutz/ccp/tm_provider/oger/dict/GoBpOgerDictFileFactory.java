package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class GoBpOgerDictFileFactory extends OgerDictFileFactory {
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";
	private static final String CELLULAR_COMPONENT = "http://purl.obolibrary.org/obo/GO_0005575";
	private static final String MOLECULAR_FUNCTION = "http://purl.obolibrary.org/obo/GO_0003674";

	public GoBpOgerDictFileFactory() {
		super("biological_process", "GO_BP", SynonymSelection.EXACT_ONLY,
				Arrays.asList(MOLECULAR_FUNCTION, CELLULAR_COMPONENT));
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "GO_0003002", // region
					OBO_PURL + "GO_0023052", // signal
					OBO_PURL + "GO_0035282", // segments
					OBO_PURL + "GO_0046960" // sensitization

			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {

		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);

		if (iri.equals("http://purl.obolibrary.org/obo/GO_0000380")) {
			toReturn.add("alternative splicing");
		}

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

}
