package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class NcbiTaxonOgerDictFileFactory extends OgerDictFileFactory {

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private static final String TAXONOMIC_RANK_IRI = "http://purl.obolibrary.org/obo/NCBITaxon#_taxonomic_rank";

	public static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList(TAXONOMIC_RANK_IRI);

	public NcbiTaxonOgerDictFileFactory() {
		super("organism", "NCBITaxon", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "NCBITaxon_3493", // Fig
					OBO_PURL + "NCBITaxon_169495" // This
			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
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
		map.put(OBO_PURL + "NCBITaxon_5882", new HashSet<String>(Arrays.asList("Glaucoma")));
		map.put(OBO_PURL + "NCBITaxon_1369087", new HashSet<String>(Arrays.asList("Data")));
		map.put(OBO_PURL + "NCBITaxon_1", new HashSet<String>(Arrays.asList("root")));
		map.put(OBO_PURL + "NCBITaxon_15957", new HashSet<String>(Arrays.asList("Timothy")));
		map.put(OBO_PURL + "NCBITaxon_6754", new HashSet<String>(Arrays.asList("Cancer")));
		map.put(OBO_PURL + "NCBITaxon_3554", new HashSet<String>(Arrays.asList("β", "Beta")));
		map.put(OBO_PURL + "NCBITaxon_1118549", new HashSet<String>(Arrays.asList("Electron")));
		map.put(OBO_PURL + "NCBITaxon_79338", new HashSet<String>(Arrays.asList("Codon")));
		map.put(OBO_PURL + "NCBITaxon_29278", new HashSet<String>(Arrays.asList("vectors")));
		map.put(OBO_PURL + "NCBITaxon_1233420", new HashSet<String>(Arrays.asList("vectors (genetic code 6)")));
		map.put(OBO_PURL + "NCBITaxon_3863", new HashSet<String>(Arrays.asList("Lens")));
		map.put(OBO_PURL + "NCBITaxon_9596", new HashSet<String>(Arrays.asList("Pan")));
		map.put(OBO_PURL + "NCBITaxon_49990", new HashSet<String>(Arrays.asList("Thymus")));
		map.put(OBO_PURL + "NCBITaxon_228055", new HashSet<String>(Arrays.asList("Nasa")));
		map.put(OBO_PURL + "NCBITaxon_274080", new HashSet<String>(Arrays.asList("Camera")));
		map.put(OBO_PURL + "NCBITaxon_37965", new HashSet<String>(Arrays.asList("hybrid")));
		map.put(OBO_PURL + "NCBITaxon_117893", new HashSet<String>(Arrays.asList("rays")));
		map.put(OBO_PURL + "NCBITaxon_1925465", new HashSet<String>(Arrays.asList("Major")));
		map.put(OBO_PURL + "NCBITaxon_189528", new HashSet<String>(Arrays.asList("Indicator")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

	public static void performTaxonPromotion(File csDictFile, File ciDictFile, File ncbiTaxonOwlFile)
			throws IOException, OWLOntologyCreationException {

		System.out.println("Loading owl file...");
		OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ncbiTaxonOwlFile)));

		// create temporary update files to use
		File updatedCsDictFile = new File(csDictFile.getParentFile(), csDictFile.getName() + ".update");
		File updatedCiDictFile = new File(ciDictFile.getParentFile(), ciDictFile.getName() + ".update");

		System.out.println("Performing promotion on CS file...");
		performPromotion(csDictFile, updatedCsDictFile, ontUtil);
		System.out.println("Performing promotion on CI file...");
		performPromotion(ciDictFile, updatedCiDictFile, ontUtil);

		/* rename files so that the updated files take the place of the originals */
		// first backup the original files
		File csDictBackupFile = new File(csDictFile.getParentFile(), csDictFile.getName() + ".bak");
		File ciDictBackupFile = new File(ciDictFile.getParentFile(), ciDictFile.getName() + ".bak");

		boolean renameCs = csDictFile.renameTo(csDictBackupFile);
		if (!renameCs) {
			throw new IllegalStateException("RenameCs failed");
		}
		boolean renameCi = ciDictFile.renameTo(ciDictBackupFile);
		if (!renameCi) {
			throw new IllegalStateException("RenameCi failed");
		}
		// then rename the updated files to the original file names
		boolean renameCsUpdate = updatedCsDictFile.renameTo(csDictFile);
		if (!renameCsUpdate) {
			throw new IllegalStateException("RenameCsUpdate failed");
		}
		boolean renameCiUpdate = updatedCiDictFile.renameTo(ciDictFile);
		if (!renameCiUpdate) {
			throw new IllegalStateException("RenameCiUpdate failed");
		}

	}

	private static void performPromotion(File dictFile, File updatedDictFile, OntologyUtil ontUtil)
			throws IOException, FileNotFoundException {
		Map<String, Set<String>> labelToIdMap = loadLabelToIdMap(dictFile);

		// this map will store ids to associate with a given label after ncbitaxon
		// promotion has taken place
		Map<String, Set<String>> labelToKeepIdMap = new HashMap<String, Set<String>>();
		for (Entry<String, Set<String>> entry : labelToIdMap.entrySet()) {
			String label = entry.getKey();
			Set<String> ids = entry.getValue();
			if (ids.size() > 1) {
				Set<String> preferredIds = prefer(ids, ontUtil, label);
				labelToKeepIdMap.put(label, preferredIds);
			}
		}

		System.out.println("Writing output file...");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(updatedDictFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(dictFile, CharacterEncoding.UTF_8); lineIter
					.hasNext();) {
				Line line = lineIter.next();
				String[] cols = line.getText().split("\\t");

				String id = cols[2];
				String label = cols[3];

				if (labelToKeepIdMap.containsKey(label)) {
					if (labelToKeepIdMap.get(label).contains(id)) {
						writer.write(line.getText() + "\n");
					}
				} else {
					writer.write(line.getText() + "\n");
				}
			}
		}
	}

	private static Map<String, Set<String>> loadLabelToIdMap(File dictFile) throws IOException {

		Map<String, Set<String>> labelToIdMap = new HashMap<String, Set<String>>();
		for (StreamLineIterator lineIter = new StreamLineIterator(dictFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 100000 == 0) {
				System.out.println("load label-to-id map progress: " + line.getLineNumber());
			}

			String[] cols = line.getText().split("\\t");

			String id = cols[2];
			String label = cols[3];

			CollectionsUtil.addToOne2ManyUniqueMap(label, id, labelToIdMap);
		}

		return labelToIdMap;
	}

	protected static Set<String> prefer(Set<String> ids, OntologyUtil ontUtil, String label) {

		Set<String> toKeep = new HashSet<String>(ids);

		List<String> idList = new ArrayList<String>(ids);

		Map<OWLClass, Set<OWLClass>> clsToAncestorMap = new HashMap<OWLClass, Set<OWLClass>>();

		Set<String> removed = new HashSet<String>();

		if (idList.size() > 20) {
			System.out.println(String.format("Returning one random id (%d) for label: %s", idList.size(), label));
			return new HashSet<String>(Arrays.asList(idList.get(0)));
		}

		for (int i = 0; i < idList.size(); i++) {
			String id1 = idList.get(i);

			if (!removed.contains(id1)) {
				OWLClass owlClass1 = ontUtil.getOWLClassFromId(toOboPurl(id1));
				Set<OWLClass> ancestors1 = getAncestors(ontUtil, clsToAncestorMap, owlClass1);

				for (int j = 0; j < idList.size(); j++) {
					if (i != j) {
						String id2 = idList.get(j);
						if (!removed.contains(id2)) {
							OWLClass owlClass2 = ontUtil.getOWLClassFromId(toOboPurl(id2));
							if (ancestors1.contains(owlClass2)) {
								toKeep.remove(id1);
								removed.add(id1);
							} else {
								Set<OWLClass> ancestors2 = getAncestors(ontUtil, clsToAncestorMap, owlClass2);
								if (ancestors2.contains(owlClass1)) {
									toKeep.remove(id2);
									removed.add(id2);
								}
							}
						}
					}
				}
				// if there is only one member in toKeep, then we can break out of the loops.
				if (toKeep.size() == 1) {
					break;
				}
			}
		}

		return toKeep;
	}

	private static Set<OWLClass> getAncestors(OntologyUtil ontUtil, Map<OWLClass, Set<OWLClass>> clsToAncestorMap,
			OWLClass owlClass) {
		Set<OWLClass> ancestors = null;
		if (clsToAncestorMap.containsKey(owlClass)) {
			ancestors = clsToAncestorMap.get(owlClass);
		} else {
			ancestors = ontUtil.getAncestors(owlClass);
			clsToAncestorMap.put(owlClass, ancestors);
		}
		return ancestors;
	}

	private static String toOboPurl(String curie) {
		return String.format("http://purl.obolibrary.org/obo/%s", curie.replace(":", "_"));
	}

	// this is run from the OgerDictFileFactory
//	public static void main(String[] args) {
//		File dir = new File("/Users/bill/projects/ncats-translator/prototype/oger-docker.git/dict/20230716");
//		File csDictFile = new File(dir, "NCBITaxon.case_sensitive.tsv");
//		File ciDictFile = new File(dir, "NCBITaxon.case_insensitive.tsv");
//
////		File updatedCsDictFile = new File(dir, "NCBITaxon.case_sensitive.updated.tsv");
////		File updatedCiDictFile = new File(dir, "NCBITaxon.case_insensitive.updated.tsv");
//
//		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");
//		File ncbiTaxonOwlFile = new File(ontBase, "ncbitaxon.owl.gz");
//		try {
//			performTaxonPromotion(csDictFile, ciDictFile, ncbiTaxonOwlFile);
//		} catch (IOException | OWLOntologyCreationException e) {
//			e.printStackTrace();
//		}
//
//	}

}
