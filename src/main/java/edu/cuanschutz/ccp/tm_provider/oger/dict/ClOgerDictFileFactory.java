package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class ClOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public ClOgerDictFileFactory() {
		super("cell", "CL", SynonymSelection.EXACT_ONLY, null);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "CL_0000000", // cell
					OBO_PURL + "CL_0000378", // supporting cells
					OBO_PURL + "CL_0000619" // supporting cells
			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		if (iri.equals("CL:0000601")) { // cochlear outer hair cell
			toReturn.add("outer hair cell");
		}
		if (iri.equals("CL:0000589")) { // cochlear inner hair cell
			toReturn.add("inner hair cell");
		}
		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}
		return toReturn;
	}

}
