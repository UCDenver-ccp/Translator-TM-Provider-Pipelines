package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.nlp.core.util.StopWordUtil;

public class MondoOgerDictFileFactory extends OgerDictFileFactory {

	private static final String DISEASE_CHARACTERISTIC = "http://purl.obolibrary.org/obo/MONDO_0021125";
	private static final String DEFECT = "http://purl.obolibrary.org/obo/MONDO_0008568";
	private static final String THYROID_TUMOR = "http://purl.obolibrary.org/obo/MONDO_0015074";

	public MondoOgerDictFileFactory() {
		super("disease", "MONDO", SynonymSelection.EXACT_PLUS_RELATED, Arrays.asList(DISEASE_CHARACTERISTIC));
	}

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns) {
		Set<String> toReturn = new HashSet<String>(syns);

		// remove stopwords
		Set<String> stopwords = new HashSet<String>(StopWordUtil.STOPWORDS);
		Set<String> toRemove = new HashSet<String>();
		for (String syn : toReturn) {
			if (stopwords.contains(syn.toLowerCase())) {
				toRemove.add(syn);
			}
		}
		toReturn.removeAll(toRemove);

		// for all classes that are "... virus infection" -- add a synonym that is just
		// "... virus"
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

		// remove ", formerly" or "(formerly)"
		toAdd = new HashSet<String>();
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

		// remove single character synonyms
		toRemove = new HashSet<String>();
		for (String syn : toReturn) {
			if (syn.length() == 1) {
				toRemove.add(syn);
			}
		}
		toReturn.removeAll(toRemove);

		return toReturn;
	}

	@Override
	protected Set<String> filterSynonyms(String iri, Set<String> syns) {

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (iri.equals(DEFECT)) {
			updatedSyns.remove("defect");
		}

		if (iri.equals(THYROID_TUMOR)) {
			updatedSyns.remove("THYROID");
		}

		return updatedSyns;
	}

}
