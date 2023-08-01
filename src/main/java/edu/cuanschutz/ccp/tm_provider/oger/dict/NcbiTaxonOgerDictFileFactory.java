package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;

public class NcbiTaxonOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private static final String TAXONOMIC_RANK_IRI = "http://purl.obolibrary.org/obo/NCBITaxon#_taxonomic_rank";

	public static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(TAXONOMIC_RANK_IRI);
	
	

	public NcbiTaxonOgerDictFileFactory() {
		super("organism", "NCBITaxon", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList(OBO_PURL + "NCBITaxon_3493" // Fig
	));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		if (iri.equals(OBO_PURL + "NCBITaxon_6239")) {
			toReturn.add("C. elegans");
		}

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "NCBITaxon_201850", new HashSet<String>(Arrays.asList("Car")));
		map.put(OBO_PURL + "NCBITaxon_5882", new HashSet<String>(Arrays.asList("glaucoma")));
		map.put(OBO_PURL + "NCBITaxon_1369087", new HashSet<String>(Arrays.asList("data")));
		map.put(OBO_PURL + "NCBITaxon_1", new HashSet<String>(Arrays.asList("root")));
		map.put(OBO_PURL + "NCBITaxon_15957", new HashSet<String>(Arrays.asList("Timothy")));
		map.put(OBO_PURL + "NCBITaxon_6754", new HashSet<String>(Arrays.asList("cancer")));
		map.put(OBO_PURL + "NCBITaxon_3554", new HashSet<String>(Arrays.asList("β", "beta")));
		map.put(OBO_PURL + "NCBITaxon_1118549", new HashSet<String>(Arrays.asList("electron")));
		map.put(OBO_PURL + "NCBITaxon_79338", new HashSet<String>(Arrays.asList("codon")));
		map.put(OBO_PURL + "NCBITaxon_29278", new HashSet<String>(Arrays.asList("vectors")));
		map.put(OBO_PURL + "NCBITaxon_1233420", new HashSet<String>(Arrays.asList("vectors")));
		map.put(OBO_PURL + "NCBITaxon_3863", new HashSet<String>(Arrays.asList("lens")));
		map.put(OBO_PURL + "NCBITaxon_9596", new HashSet<String>(Arrays.asList("pan")));
		map.put(OBO_PURL + "NCBITaxon_49990", new HashSet<String>(Arrays.asList("thymus")));
		map.put(OBO_PURL + "NCBITaxon_228055", new HashSet<String>(Arrays.asList("Nasa")));
		map.put(OBO_PURL + "NCBITaxon_274080", new HashSet<String>(Arrays.asList("camera")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
