package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import lombok.Data;

/**
 * Creates a mapping from classes in the protein ontology to their corresponding
 * 'gene' level class which is species non-specific.
 *
 */
@Data
public class NcbiTaxonPromotionMapFactory {

	private final OntologyUtil ontUtil;

	public void createMappingFile(BufferedWriter writer) throws IOException {

		int count = 0;
		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			if (count++ % 10000 == 0) {
				System.out.println(String.format("progress: %d...", count - 1));
			}
			OWLClass cls = classIterator.next();

			Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
			if (ancestors != null && !ancestors.isEmpty()) {
				StringBuilder sb = new StringBuilder();
				for (OWLClass ancestorCls : ancestors) {
					sb.append("|" + getId(ancestorCls));
				}
				writeMapping(writer, getId(cls), sb.substring(1));
			}
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
//		File ncbitaxonOwlFile = new File(
//				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20210918/ncbitaxon.owl.gz");
//		File outputFile = new File(
//				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20210918/ncbitaxon-promotion-map.tsv");

		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");

		File ncbitaxonOwlFile = new File(ontBase, "ncbitaxon.owl");
		File outputFile = new File(ontBase, "ncbitaxon-promotion-map.tsv");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			OntologyUtil ontUtil = new OntologyUtil(ncbitaxonOwlFile);
//			OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ncbitaxonOwlFile)));
			new NcbiTaxonPromotionMapFactory(ontUtil).createMappingFile(writer);

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

}
