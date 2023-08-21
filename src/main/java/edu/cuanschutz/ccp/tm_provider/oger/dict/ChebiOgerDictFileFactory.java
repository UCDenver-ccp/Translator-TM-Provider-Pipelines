package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLAnnotationValue;
import org.semanticweb.owlapi.model.OWLClass;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class ChebiOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private static final String CHEBI_FORMULA_IRI = "http://purl.obolibrary.org/obo/chebi/formula";

	private static final String CHEBI_ROLE = "http://purl.obolibrary.org/obo/CHEBI_50906";
	private static final String CHEBI_SUBATOMIC_PARTICLE = "http://purl.obolibrary.org/obo/CHEBI_36342";
	private static final String CHEBI_ATOM = "http://purl.obolibrary.org/obo/CHEBI_33250";
	private static final String CHEBI_GROUP = "http://purl.obolibrary.org/obo/CHEBI_24433";
	private static final String CHEBI_ALPHA_AMINO_ACID = "http://purl.obolibrary.org/obo/CHEBI_33704";

	public static List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(CHEBI_ROLE, CHEBI_SUBATOMIC_PARTICLE, CHEBI_ATOM,
			CHEBI_GROUP, CHEBI_ALPHA_AMINO_ACID);

	public ChebiOgerDictFileFactory() {
		super("chemical", "CHEBI", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "CHEBI_15035", // retinal
					OBO_PURL + "CHEBI_18367", // phosphate
					OBO_PURL + "CHEBI_26020", // phosphate
					OBO_PURL + "CHEBI_36976", // nucleotide
					OBO_PURL + "CHEBI_16670", // peptide
					OBO_PURL + "CHEBI_7998", // peptide
					OBO_PURL + "CHEBI_7999", // peptide
					OBO_PURL + "CHEBI_8001", // peptide
					OBO_PURL + "CHEBI_36080", // protein
					OBO_PURL + "CHEBI_8580", // protein
					OBO_PURL + "CHEBI_2645", // amino acid
					OBO_PURL + "CHEBI_33709", // amino acid
					OBO_PURL + "CHEBI_33731", // Cluster
					OBO_PURL + "CHEBI_8762", // RNA
					OBO_PURL + "CHEBI_60004", // mixture
					OBO_PURL + "CHEBI_5386", // globin
					OBO_PURL + "CHEBI_33696", // nucleic acid
					OBO_PURL + "CHEBI_49807", // lead
					OBO_PURL + "CHEBI_15841", // polypeptides
					OBO_PURL + "CHEBI_79381", // a-factor
					OBO_PURL + "CHEBI_18059", // lipid
					OBO_PURL + "CHEBI_75958", // solution
					OBO_PURL + "CHEBI_27889", // lead
					OBO_PURL + "CHEBI_24870", // ion
					OBO_PURL + "CHEBI_15377", // water
					OBO_PURL + "CHEBI_25367", // molecule
					OBO_PURL + "CHEBI_36357", // compound
					OBO_PURL + "CHEBI_7754", // oligonucleotide
					OBO_PURL + "CHEBI_4705", // double-stranded DNA
					OBO_PURL + "CHEBI_9160" // single-stranded DNA

			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSynonyms(toReturn);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		addChemicalFormula(iri, toReturn, ontUtil);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		if (iri.equals(OBO_PURL + "CHEBI_26710")) {
			toReturn.add("NaCl"); // formula in chebi.owl is ClNa
		}

		return toReturn;
	}

	private void addChemicalFormula(String iri, Set<String> toReturn, OntologyUtil ontUtil) {

		OWLClass cls = ontUtil.getOWLClassFromId(iri);

		Collection<OWLAnnotationValue> values = ontUtil.getAnnotationPropertyValues(cls, CHEBI_FORMULA_IRI);
		for (OWLAnnotationValue value : values) {
			String formula = value.toString();
			if (formula.startsWith("\"")) {
				formula = StringUtil.removePrefix(formula, "\"");
			}
			if (formula.endsWith("\"^^xsd:string")) {
				formula = StringUtil.removeSuffix(formula, "\"^^xsd:string");
			}
			if (formula.endsWith("\"")) {
				formula = StringUtil.removeSuffix(formula, "\"");
			}
			toReturn.add(formula);
		}
	}

	private Set<String> filterSynonyms(Set<String> synonyms) {
		// remove any synonym with 4 or more hyphens or 3 or more commas -- this will
		// exclude complicated
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

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "CHEBI_37409", new HashSet<String>(Arrays.asList("crown")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
