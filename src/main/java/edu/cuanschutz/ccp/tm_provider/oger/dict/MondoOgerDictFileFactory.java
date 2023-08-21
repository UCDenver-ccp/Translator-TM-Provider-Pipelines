package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class MondoOgerDictFileFactory extends OgerDictFileFactory {

	private static final String DISEASE_CHARACTERISTIC = "http://purl.obolibrary.org/obo/MONDO_0021125";
	private static final String DISEASE_SUSCEPTIBILITY = "http://purl.obolibrary.org/obo/MONDO_0042489";

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public static List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(DISEASE_CHARACTERISTIC, DISEASE_SUSCEPTIBILITY);

	public MondoOgerDictFileFactory() {
		super("disease", "MONDO", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "MONDO_0000001", // disease
					OBO_PURL + "MONDO_0002254", // syndrome
					OBO_PURL + "MONDO_0021178", // injury
					OBO_PURL + "MONDO_0006025", // autosomal recessive condition
					OBO_PURL + "MONDO_0000429", // autosomal genetic disease
					OBO_PURL + "MONDO_0003847", // Mendelian disease
					OBO_PURL + "MONDO_0700096", // human disease or disorder
					OBO_PURL + "MONDO_0020683", // acute disease
					OBO_PURL + "MONDO_0002409", // auditory system disorder
					OBO_PURL + "MONDO_0002657", // breast disorder
					OBO_PURL + "MONDO_0045024", // cancer or benign tumor
					OBO_PURL + "MONDO_0004995", // cardiovascular disorder
					OBO_PURL + "MONDO_0003900", // connective tissue disorder
					OBO_PURL + "MONDO_0004335", // digestive system disorder
					OBO_PURL + "MONDO_0021147", // disorder of development or morphogenesis
					OBO_PURL + "MONDO_0002022", // disorder of orbital region
					OBO_PURL + "MONDO_0024458", // disorder of visual system
					OBO_PURL + "MONDO_0005151", // endocrine system disorder
					OBO_PURL + "MONDO_0005570", // hematologic disorder
					OBO_PURL + "MONDO_0043543", // iatrogenic disease
					OBO_PURL + "MONDO_0700007", // idiopathic disease
					OBO_PURL + "MONDO_0005046", // immune system disorder
					OBO_PURL + "MONDO_0021166", // inflammatory disease
					OBO_PURL + "MONDO_0002051", // integumentary system disorder
					OBO_PURL + "MONDO_0005066", // metabolic disease
					OBO_PURL + "MONDO_0044970", // mitochondrial disease
					OBO_PURL + "MONDO_0006858", // mouth disorder
					OBO_PURL + "MONDO_0002081", // musculoskeletal system disorder
					OBO_PURL + "MONDO_0005071", // nervous system disorder
					OBO_PURL + "MONDO_0005137", // nutritional disorder
					OBO_PURL + "MONDO_0700003", // obstetric disorder
					OBO_PURL + "MONDO_0100366", // occupational disorder
					OBO_PURL + "MONDO_0024623", // otorhinolaryngologic disease
					OBO_PURL + "MONDO_0100086", // perinatal disease
					OBO_PURL + "MONDO_0029000", // poisoning
					OBO_PURL + "MONDO_0002025", // psychiatric disorder
					OBO_PURL + "MONDO_0043459", // radiation-induced disorder
					OBO_PURL + "MONDO_0005039", // reproductive system disorder
					OBO_PURL + "MONDO_0005087", // respiratory system disorder
					OBO_PURL + "MONDO_0044991", // upper digestive tract disorder
					OBO_PURL + "MONDO_0002118" // urinary system disorder
			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);

		augmentVirusSynonyms(toReturn);
		augmentSynonymsWithFormerly(toReturn);
		augmentHemoSynonyms(toReturn);
		augmentHeartSynonyms(toReturn);
		// remove single character synonyms
		toReturn = removeWordsLessThenLength(toReturn, 4);
		toReturn = filterSpecificSynonyms(iri, toReturn);

		if (iri.equals(OBO_PURL + "MONDO_0005129")) { // cataract
			toReturn.add("cataracts");
		}
		if (iri.equals(OBO_PURL + "MONDO_0005044")) {
			toReturn.add("hypertension");
		}

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	/**
	 * For synonyms that have "formerly" in them, add a synonym where "formerly" is
	 * removed
	 * 
	 * @param toReturn
	 */
	private void augmentSynonymsWithFormerly(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.contains(", formerly")) {
				System.out.println("Adding: " + syn.replace(", formerly", ""));
				toAdd.add(syn.replace(", formerly", ""));
			} else if (syn.contains("(formerly)")) {
				System.out.println("Adding: " + syn.replace("(formerly)", ""));
				toAdd.add(syn.replace("(formerly)", ""));
			}
		}
		toReturn.addAll(toAdd);
	}

	/**
	 * for all classes that are "X virus infection" -- add a synonym that is just "X
	 * virus"
	 * 
	 * @param toReturn
	 */
	private void augmentVirusSynonyms(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.endsWith("virus infection")) {
				String virus = StringUtil.removeSuffix(syn, " infection");
				toAdd.add(virus);
			} else if (syn.endsWith("virus infections")) {
				String virus = StringUtil.removeSuffix(syn, " infections");
				toAdd.add(virus);
			}
		}
		toReturn.addAll(toAdd);
	}

	/**
	 * If the synonym starts with hemo, add a synonym that stats with haemo
	 * 
	 * @param toReturn
	 */
	private void augmentHemoSynonyms(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.startsWith("hemo")) {
				String haemo = syn.replace("hemo", "haemo");
				toAdd.add(haemo);
			}
		}
		toReturn.addAll(toAdd);
	}

	/**
	 * If the synonym contains heart, create a new synonym that uses "cardiac" in
	 * its place
	 * 
	 * @param toReturn
	 */
	private void augmentHeartSynonyms(Set<String> toReturn) {
		Set<String> toAdd = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.contains("heart")) {
				String cardiac = syn.replace("heart", "cardiac");
				toAdd.add(cardiac);
			}
		}
		toReturn.addAll(toAdd);
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "MONDO_0005059", new HashSet<String>(Arrays.asList("blood")));
		map.put(OBO_PURL + "MONDO_0008568", new HashSet<String>(Arrays.asList("defect")));
		map.put(OBO_PURL + "MONDO_0015074", new HashSet<String>(Arrays.asList("THYROID")));
		map.put(OBO_PURL + "MONDO_0007620", new HashSet<String>(Arrays.asList("fed"))); // fish eye disease
		map.put(OBO_PURL + "MONDO_0000179", new HashSet<String>(Arrays.asList("nuclear localization signal"))); //
		map.put(OBO_PURL + "MONDO_0044688", new HashSet<String>(Arrays.asList("ion"))); //
		map.put(OBO_PURL + "MONDO_0015404", new HashSet<String>(Arrays.asList("rich"))); //
		map.put(OBO_PURL + "MONDO_0005002", new HashSet<String>(Arrays.asList("cold"))); //
		map.put(OBO_PURL + "MONDO_0006767", new HashSet<String>(Arrays.asList("gave"))); //
		map.put(OBO_PURL + "MONDO_0005386", new HashSet<String>(Arrays.asList("pad"))); //
		map.put(OBO_PURL + "MONDO_0010953", new HashSet<String>(Arrays.asList("face"))); //
		map.put(OBO_PURL + "MONDO_0005047", new HashSet<String>(Arrays.asList("sterile"))); //
		map.put(OBO_PURL + "MONDO_0009994", new HashSet<String>(Arrays.asList("arms"))); //
		map.put(OBO_PURL + "MONDO_0007127", new HashSet<String>(Arrays.asList("dish"))); //
		map.put(OBO_PURL + "MONDO_0019065", new HashSet<String>(Arrays.asList("amyloid"))); //
		map.put(OBO_PURL + "MONDO_0015595", new HashSet<String>(Arrays.asList("pale"))); //
		map.put(OBO_PURL + "MONDO_0006466", new HashSet<String>(Arrays.asList("settle"))); //
		map.put(OBO_PURL + "MONDO_0014493", new HashSet<String>(Arrays.asList("chai"))); //
		map.put(OBO_PURL + "MONDO_0004938", new HashSet<String>(Arrays.asList("dependence"))); //
		map.put(OBO_PURL + "MONDO_0015285", new HashSet<String>(Arrays.asList("lamb"))); //

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
