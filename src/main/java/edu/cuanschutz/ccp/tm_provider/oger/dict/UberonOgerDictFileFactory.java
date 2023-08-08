package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class UberonOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private static final String LIFE_CYCLE_IRI = "http://purl.obolibrary.org/obo/UBERON_0000104";

	public static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(LIFE_CYCLE_IRI);

	public UberonOgerDictFileFactory() {
		super("anatomy", "UBERON", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "UBERON_2000106", // extension
					OBO_PURL + "UBERON_0004529", // projection
					OBO_PURL + "UBERON_0000914", // organismal segment
					OBO_PURL + "UBERON_0000025", // tube
					OBO_PURL + "UBERON_0002542", // scale
					OBO_PURL + "UBERON_0002415", // tail
					OBO_PURL + "UBERON_0010164", // collection of hair
					OBO_PURL + "UBERON_0000014", // zone of skin
					OBO_PURL + "UBERON_0000026", // appendage
					OBO_PURL + "UBERON_0000170" // pair of lungs

			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		if (iri.equals(OBO_PURL + "UBERON_0000970")) { // eye
			toReturn.add("eyes");
		} else if (iri.equals(OBO_PURL + "UBERON_0001690")) { // ear
			toReturn.add("ears");
		}

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "UBERON_2001463", new HashSet<String>(Arrays.asList("bars")));
		map.put(OBO_PURL + "UBERON_0014402", new HashSet<String>(Arrays.asList("sex-specific", "gender-specific")));
		map.put(OBO_PURL + "UBERON_2000859", new HashSet<String>(Arrays.asList("ha(pu)", "ha")));
		map.put(OBO_PURL + "UBERON_0003062", new HashSet<String>(Arrays.asList("shield", "organizer")));
		map.put(OBO_PURL + "UBERON_0007380", new HashSet<String>(Arrays.asList("scales")));
		map.put(OBO_PURL + "UBERON_0001093", new HashSet<String>(Arrays.asList("axis")));
		map.put(OBO_PURL + "UBERON_2000271", new HashSet<String>(Arrays.asList("radials")));
		map.put(OBO_PURL + "UBERON_0000104", new HashSet<String>(Arrays.asList("life")));
		map.put(OBO_PURL + "UBERON_2000006", new HashSet<String>(Arrays.asList("ball")));
		map.put(OBO_PURL + "UBERON_0001137", new HashSet<String>(Arrays.asList("back")));
		map.put(OBO_PURL + "UBERON_2001840", new HashSet<String>(Arrays.asList("tip")));
		map.put(OBO_PURL + "UBERON_2002284", new HashSet<String>(Arrays.asList("markings")));
		map.put(OBO_PURL + "UBERON_2000438", new HashSet<String>(Arrays.asList("phy")));
		map.put(OBO_PURL + "UBERON_0002488", new HashSet<String>(Arrays.asList("helix (auricula)", "helix")));
		map.put(OBO_PURL + "UBERON_0000180", new HashSet<String>(Arrays.asList("lateral region")));
		map.put(OBO_PURL + "UBERON_2001463", new HashSet<String>(Arrays.asList("bar", "stripe", "stripes")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
