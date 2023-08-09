package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLAnnotationValue;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

/**
 * This class was originally designed to create a mapping from HP to MONDO. For
 * now, we will instead simply remove any HP annotations that directly overlap
 * with MONDO. The mappings between HP and MONDO are possible, but would require
 * 975 manual checks for classes that are mapped indirectly via MeSH, Snomed, or
 * UMLS where their labels are not exact.
 *
 */
public class MondoToHpEquivalenceMapFileFactory {

	private static final String SKOS_EXACT_MATCH_IRI = "http://www.w3.org/2004/02/skos/core#exactMatch";
	private static final String OBO_IN_OWL_HAS_DB_XREF_IRI = "http://www.geneontology.org/formats/oboInOwl#hasDbXref";
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public static void createHpToMondoMap(File mondoOwlFile, File hpOwlFile, File outputFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {

		Map<String, String> hpIdToLabelMap = new HashMap<String, String>();
		Map<String, String> mondoIdToLabelMap = new HashMap<String, String>();

		Map<String, Set<String>> hpToMondoMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> directHpToMondoMap = new HashMap<String, Set<String>>();

		Map<String, Set<String>> idToMondoOrHpIdMap = new HashMap<String, Set<String>>();

		System.out.println("Loading MONDO...");
		OntologyUtil ontUtil = mondoOwlFile.getName().endsWith(".gz")
				? new OntologyUtil(new GZIPInputStream(new FileInputStream(mondoOwlFile)))
				: new OntologyUtil(mondoOwlFile);

		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			OWLClass cls = classIterator.next();
			if (cls.getIRI().toString().contains("MONDO_")) {

				String mondoId = getId(cls);
				String label = ontUtil.getLabel(cls);

				mondoIdToLabelMap.put(mondoId, removeQuotes(label));

				Collection<OWLAnnotationValue> values = ontUtil.getAnnotationPropertyValues(cls, SKOS_EXACT_MATCH_IRI);
				for (OWLAnnotationValue value : values) {
					System.out.println("SKOS: " + value.toString());

					String curie = null;
					if (value.toString().contains("http://identifiers.org/mesh/")) {
						curie = "MSH:" + StringUtil.removePrefix(value.toString(), "http://identifiers.org/mesh/");
					}
					if (value.toString().contains("http://linkedlifedata.com/resource/umls/id/")) {
						curie = "UMLS:" + StringUtil.removePrefix(value.toString(),
								"http://linkedlifedata.com/resource/umls/id/");
					}
					if (value.toString().contains("http://identifiers.org/snomedct/")) {
						curie = "SNOMEDCT_US:"
								+ StringUtil.removePrefix(value.toString(), "http://identifiers.org/snomedct/");
					}

					if (curie != null) {
						CollectionsUtil.addToOne2ManyUniqueMap(curie, mondoId, idToMondoOrHpIdMap);
					}
				}

				values = ontUtil.getAnnotationPropertyValues(cls, OBO_IN_OWL_HAS_DB_XREF_IRI);
				for (OWLAnnotationValue value : values) {
					if (value.toString().contains("HP:")) {
						CollectionsUtil.addToOne2ManyUniqueMap(removeQuotes(value.toString()), getId(cls),
								hpToMondoMap);

						CollectionsUtil.addToOne2ManyUniqueMap(removeQuotes(value.toString()), getId(cls),
								directHpToMondoMap);
					}
				}
			}
		}

		System.out.println("Loading HP...");
		ontUtil = hpOwlFile.getName().endsWith(".gz")
				? new OntologyUtil(new GZIPInputStream(new FileInputStream(hpOwlFile)))
				: new OntologyUtil(hpOwlFile);

		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			OWLClass cls = classIterator.next();
			if (cls.getIRI().toString().contains("HP_")) {
				String hpId = getId(cls);
				String label = ontUtil.getLabel(cls);

				hpIdToLabelMap.put(hpId, removeQuotes(label));

				Collection<OWLAnnotationValue> values = ontUtil.getAnnotationPropertyValues(cls,
						OBO_IN_OWL_HAS_DB_XREF_IRI);
				for (OWLAnnotationValue value : values) {
					CollectionsUtil.addToOne2ManyUniqueMap(removeQuotes(value.toString()), hpId, idToMondoOrHpIdMap);
				}
			}
		}

		for (Entry<String, Set<String>> entry : idToMondoOrHpIdMap.entrySet()) {
			Set<String> mondoHpIdSet = entry.getValue();

			if (mondoHpIdSet.size() > 1) {
				Map<Ont, Set<String>> map = new HashMap<Ont, Set<String>>();
				for (String id : mondoHpIdSet) {
					if (id.startsWith("HP:")) {
						CollectionsUtil.addToOne2ManyUniqueMap(Ont.HP, id, map);
					} else if (id.startsWith("MONDO:")) {
						CollectionsUtil.addToOne2ManyUniqueMap(Ont.MONDO, id, map);
					}
				}

				if (map.containsKey(Ont.HP) && map.containsKey(Ont.MONDO)) {
					for (String hpId : map.get(Ont.HP)) {
						for (String mondoId : map.get(Ont.MONDO)) {
							CollectionsUtil.addToOne2ManyUniqueMap(hpId, mondoId, hpToMondoMap);
						}
					}
				}

			}
		}

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			for (Entry<String, Set<String>> entry : hpToMondoMap.entrySet()) {
				String hpId = entry.getKey();
				String mondoIds = CollectionsUtil.createDelimitedString(entry.getValue(), "|");

				String hpLabel = hpIdToLabelMap.get(hpId);

				List<String> mondoLabelsList = new ArrayList<String>();
				boolean directConnection = false;
				boolean exactLabelMatch = false;
				for (String mondoId : mondoIds.split("\\|")) {
					if (directHpToMondoMap.containsKey(hpId) && directHpToMondoMap.get(hpId).contains(mondoId)) {
						directConnection = true;
					}
					String mondoLabel = mondoIdToLabelMap.get(mondoId);
					if (hpLabel != null && mondoLabel != null) {
						if (hpLabel.equalsIgnoreCase(mondoLabel)) {
							exactLabelMatch = true;
						}
					} else {
						System.out.println(String.format("Null label detected: %s %s -- %s %s", hpId, hpLabel, mondoId,
								mondoLabel));
					}

					mondoLabelsList.add(mondoLabel);
				}
				String mondoLabels = CollectionsUtil.createDelimitedString(mondoLabelsList, "|");

				String indicator = "";
				if (directConnection) {
					indicator = "**";
				} else if (exactLabelMatch) {
					indicator = "####";
				}

				writer.write(String.format("%s\t%s\t%s\t%s\t%s\n", indicator, hpId, mondoIds, hpLabel, mondoLabels));

			}
		}

	}

	private static String removeQuotes(String s) {
		if (s.startsWith("\"")) {
			s = StringUtil.removePrefix(s, "\"");
		}
		if (s.endsWith("\"")) {
			s = StringUtil.removeSuffix(s, "\"");
		}
		return s;
	}

	private enum Ont {
		HP, MONDO
	}

	private static String getId(OWLClass cls) {
		String curie = StringUtil.removePrefix(cls.getIRI().toString(), OBO_PURL);
		curie = curie.replace("_", ":");
		return curie;
	}

	public static void main(String[] args) {

		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");
		File mondoOwlFile = new File(ontBase, "mondo.owl.gz");
		File hpOwlFile = new File(ontBase, "hp.owl.gz");

		File outputFile = new File(ontBase, "hp-to-mondo-map.tsv");

		try {
			createHpToMondoMap(mondoOwlFile, hpOwlFile, outputFile);
		} catch (OWLOntologyCreationException | IOException e) {
			e.printStackTrace();
		}

	}

}
