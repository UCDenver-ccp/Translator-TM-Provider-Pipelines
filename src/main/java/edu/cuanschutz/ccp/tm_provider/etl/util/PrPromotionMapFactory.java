package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
public class PrPromotionMapFactory {

	private final OntologyUtil ontUtil;

	public void createMappingFile(BufferedWriter writer) throws IOException {

		int count = 0;
		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			if (count++ % 10000 == 0) {
				System.out.println(String.format("progress: %d...", count - 1));
			}
			OWLClass cls = classIterator.next();

			if (isGeneLevel(cls)) {
				// no need to create a mapping
				continue;
			}

			Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
			// if there is an ancestor at the Category=gene level, then return its id
			Set<OWLClass> geneLevelAncestors = new HashSet<OWLClass>();
			for (OWLClass ancestorCls : ancestors) {
				if (isGeneLevel(ancestorCls)) {
					geneLevelAncestors.add(ancestorCls);
				}
			}

			// if there is only one gene-level ancestor, then that's the one we want.
			// Otherwise, we want the lowest level (farthest from the root) gene-level
			// ancestor
			if (!geneLevelAncestors.isEmpty()) {
				OWLClass geneLevelCls = geneLevelAncestors.iterator().next();
				if (geneLevelAncestors.size() > 1) {
					for (OWLClass cls1 : geneLevelAncestors) {
						if (!cls1.equals(geneLevelCls)) {
							Set<OWLClass> ancestors1 = ontUtil.getAncestors(cls1);
							if (ancestors1.contains(geneLevelCls)) {
								geneLevelCls = cls1;
							}
						}
					}
				}

				writeMapping(writer, getId(cls), getId(geneLevelCls));

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

	private boolean isGeneLevel(OWLClass cls) {
		List<String> comments = ontUtil.getComments(cls);
		if (comments != null && !comments.isEmpty()) {
			for (String comment : comments) {
				if (comment.contains("Category=gene.")) {
					return true;
				}
			}
		}
		return false;
	}

	public static void main(String[] args) {
//		File prOwlFile = new File(
//				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20210918/pr.owl.gz");
//
//		File outputFile = new File(
//				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20210918/pr-promotion-map.tsv");

		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");

		File prOwlFile = new File(ontBase, "pr.owl");

		File outputFile = new File(ontBase, "pr-promotion-map.tsv");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			OntologyUtil ontUtil = new OntologyUtil(prOwlFile);
//			OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(prOwlFile)));
			new PrPromotionMapFactory(ontUtil).createMappingFile(writer);

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
