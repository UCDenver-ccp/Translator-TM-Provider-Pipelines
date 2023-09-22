package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import lombok.Data;

/**
 * Creates a mapping from ontology classes to a relevant Biolink concept
 * (category)
 *
 */
@Data
public class OntologyClassBiolinkCategoryMapFactory {

	public void createMappingFile(OntologyUtil ontUtil, BufferedWriter writer, Map<String, String> prefixToBiolinkMap,
			String defaultCategory) throws IOException {

		int count = 0;
		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			if (count++ % 10000 == 0) {
				System.out.println(String.format("progress: %d...", count - 1));
			}

			String category = defaultCategory;
			OWLClass cls = classIterator.next();
			String id = getId(cls);
			if (id.startsWith("GO:")) {
				// this should be a GO concept
				String namespace = ontUtil.getNamespace(cls);
				if (namespace != null) {
					if (namespace.endsWith("\"")) {
						namespace = namespace.substring(0, namespace.length() - 1);
					}
					if (namespace.equals("biological_process")) {
						category = "biolink:BiologicalProcess";
					} else if (namespace.equals("cellular_component")) {
						category = "biolink:CellularComponent";
					} else if (namespace.equals("molecular_function")) {
						category = "biolink:MolecularActivity";
					} else {
						throw new IllegalArgumentException(
								"no category for: " + cls.getIRI().toString() + " namespace = " + namespace);
					}
				} else {
					category = "";
				}
			} else if (id.contains(":")) {
				String prefix = id.substring(0, id.indexOf(":"));
				if (prefixToBiolinkMap.containsKey(prefix)) {
					category = prefixToBiolinkMap.get(prefix);
				}
			}

			if (category == null) {
				category = "biolink:Thing";
			}

			writeMapping(writer, getId(cls), category);

		}
	}

	private void writeMapping(BufferedWriter writer, String id1, String id2) throws IOException {
		writer.write(String.format("%s\t%s\n", id1, id2));
	}

	/**
	 * Convert from full URI to PREFIX:00000
	 * 
	 * @param cls
	 * @param replacedEXT
	 * @return
	 */
	private String getId(OWLClass cls) {
		String iri = cls.getIRI().toString();
		int index = iri.lastIndexOf("/") + 1;
		return iri.substring(index).replace("_", ":");
	}

	public static void main(String[] args) {

		Map<String, String> filenameToBiolinkMap = new HashMap<String, String>();

		filenameToBiolinkMap.put("chebi.owl.gz", "biolink:ChemicalSubstance");
		filenameToBiolinkMap.put("cl.owl.gz", "biolink:Cell");
		filenameToBiolinkMap.put("mop.owl.gz", "biolink:MolecularActivity");
		filenameToBiolinkMap.put("ncbitaxon.owl.gz", "biolink:OrganismTaxon");
		filenameToBiolinkMap.put("pr.owl.gz", "biolink:GeneOrGeneProduct");
		filenameToBiolinkMap.put("so.owl.gz", "biolink:SequenceFeature");
		filenameToBiolinkMap.put("uberon.owl.gz", "biolink:AnatomicalEntity");

		Map<String, String> prefixToBiolinkMap = new HashMap<String, String>();

		prefixToBiolinkMap.put("CHEBI", "biolink:ChemicalSubstance");
		prefixToBiolinkMap.put("CL", "biolink:Cell");
		prefixToBiolinkMap.put("MOP", "biolink:MolecularActivity");
		prefixToBiolinkMap.put("NCBITaxon", "biolink:OrganismTaxon");
		prefixToBiolinkMap.put("PR", "biolink:GeneOrGeneProduct");
		prefixToBiolinkMap.put("SO", "biolink:SequenceFeature");
		prefixToBiolinkMap.put("UBERON", "biolink:AnatomicalEntity");
		prefixToBiolinkMap.put("MONDO", "biolink:Disease");
		prefixToBiolinkMap.put("HP", "biolink:PhenotypicFeature");

		File ontologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies");
		File craftOntologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/craft");
		File outputFile = new File(ontologyDir, "ontology-class-biolink-category-map.tsv");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {

			for (Iterator<File> fileIterator = FileUtil.getFileIterator(craftOntologyDir, false,
					".obo.gz"); fileIterator.hasNext();) {
				File ontologyFile = fileIterator.next();
				processOntology(prefixToBiolinkMap, writer, ontologyFile);
			}

			for (Iterator<File> fileIterator = FileUtil.getFileIterator(ontologyDir, false, ".owl.gz"); fileIterator
					.hasNext();) {
				File ontologyFile = fileIterator.next();
				processOntology(prefixToBiolinkMap, writer, ontologyFile);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (OWLOntologyCreationException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static void processOntology(Map<String, String> prefixToBiolinkMap, BufferedWriter writer,
			File ontologyFile) throws OWLOntologyCreationException, IOException, FileNotFoundException {
		System.out.println("Processing " + ontologyFile.getName());
		OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));
		String defaultCategory = null;
		if (ontologyFile.getName().equals("pr.owl.gz")) {
			defaultCategory = "biolink:GeneOrGeneProduct";
		}
		new OntologyClassBiolinkCategoryMapFactory().createMappingFile(ontUtil, writer, prefixToBiolinkMap,
				defaultCategory);
	}

}
