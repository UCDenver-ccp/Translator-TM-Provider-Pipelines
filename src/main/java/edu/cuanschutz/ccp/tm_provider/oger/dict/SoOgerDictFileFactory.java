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

public class SoOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

//	private static final String AMINO_ACID_IRI = "http://purl.obolibrary.org/obo/SO_0001237";
//	private static final String POLYPEPTIDE_IRI = "http://purl.obolibrary.org/obo/SO_0000104";
//	private static final String FEATURE_ATTRIBUTE_IRI = "http://purl.obolibrary.org/obo/SO_0000733";
	private static final String SEQUENCE_ATTRIBUTE_IRI = "http://purl.obolibrary.org/obo/SO_0000400";
	private static final String SEQUENCE_COLLECTION_IRI = "http://purl.obolibrary.org/obo/SO_0001260";
	private static final String SEQUENCE_VARIANT_IRI = "http://purl.obolibrary.org/obo/SO_0001060";

	public static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(SEQUENCE_ATTRIBUTE_IRI,
			SEQUENCE_COLLECTION_IRI, SEQUENCE_VARIANT_IRI);

	public SoOgerDictFileFactory() {
		super("sequence_feature", "SO", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>();

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "SO_0000667", new HashSet<String>(Arrays.asList("insertion")));
		map.put(OBO_PURL + "SO_0000001", new HashSet<String>(Arrays.asList("region", "sequence")));
		map.put(OBO_PURL + "SO:0001236", new HashSet<String>(Arrays.asList("base")));
		map.put(OBO_PURL + "SO:0000984", new HashSet<String>(Arrays.asList("single")));
		map.put(OBO_PURL + "SO:0000699", new HashSet<String>(Arrays.asList("junction")));
		map.put(OBO_PURL + "SO:0001411", new HashSet<String>(Arrays.asList("biological region")));
		map.put(OBO_PURL + "SO:0000104", new HashSet<String>(Arrays.asList("polypeptide", "protein")));
		map.put(OBO_PURL + "SO:1000029", new HashSet<String>(Arrays.asList("deficiency")));
		map.put(OBO_PURL + "SO:0000440", new HashSet<String>(Arrays.asList("vector")));
		map.put(OBO_PURL + "SO:0000104", new HashSet<String>(Arrays.asList("protein")));
		map.put(OBO_PURL + "SO:0000804", new HashSet<String>(Arrays.asList("construct")));
		map.put(OBO_PURL + "SO:0001514", new HashSet<String>(Arrays.asList("direct")));
		map.put(OBO_PURL + "SO:0001248", new HashSet<String>(Arrays.asList("assembly")));
		map.put(OBO_PURL + "SO:0000985", new HashSet<String>(Arrays.asList("double")));
		map.put(OBO_PURL + "SO:0000856", new HashSet<String>(Arrays.asList("conserved")));
		map.put(OBO_PURL + "SO:0000051", new HashSet<String>(Arrays.asList("probe")));
		map.put(OBO_PURL + "SO:0000343", new HashSet<String>(Arrays.asList("match")));
		map.put(OBO_PURL + "SO:0000151", new HashSet<String>(Arrays.asList("clone")));
		map.put(OBO_PURL + "SO:0000856", new HashSet<String>(Arrays.asList("conserved")));
		map.put(OBO_PURL + "SO:0000731", new HashSet<String>(Arrays.asList("fragment")));
		map.put(OBO_PURL + "SO:0001516", new HashSet<String>(Arrays.asList("free")));
		map.put(OBO_PURL + "SO:0000324", new HashSet<String>(Arrays.asList("tag")));
		map.put(OBO_PURL + "SO:0001635", new HashSet<String>(Arrays.asList("upstream")));
		map.put(OBO_PURL + "SO:0000068", new HashSet<String>(Arrays.asList("overlapping")));
		map.put(OBO_PURL + "SO:0001515", new HashSet<String>(Arrays.asList("inverted")));
		map.put(OBO_PURL + "SO:0000146", new HashSet<String>(Arrays.asList("capped")));
		map.put(OBO_PURL + "SO:0000150", new HashSet<String>(Arrays.asList("read")));
		map.put(OBO_PURL + "SO:0000933", new HashSet<String>(Arrays.asList("intermediate")));
		map.put(OBO_PURL + "SO:0000814", new HashSet<String>(Arrays.asList("rescue")));
		map.put(OBO_PURL + "SO:0000119", new HashSet<String>(Arrays.asList("regulated")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

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

}
