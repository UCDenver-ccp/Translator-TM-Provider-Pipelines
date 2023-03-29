package edu.cuanschutz.ccp.tm_provider.trapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.tools.ant.util.StringUtils;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import lombok.Data;

/**
 * The ServiceProvider requested all combinations of concept prefixes that may
 * appear in the cooccurrence KP.
 *
 */
public class PrToUniProtMapFileCreator {

	private static final String RO_ONLY_IN_TAXON = "http://purl.obolibrary.org/obo/RO_0002160";
	private static final String HUMAN_TAXON_CURIE = "NCBITaxon:9606";

	public static void createPrToUniProtMapFile(File prOwlFile, File outputFile)
			throws OWLOntologyCreationException, IOException {

		InputStream is = new GZIPInputStream(new FileInputStream(prOwlFile));
		System.out.println("Loading " + prOwlFile.getName());
		OntologyUtil ontUtil = new OntologyUtil(is);
		System.out.println("load complete.");
		/*
		 * iterate over all PR concepts. If there is a UniProt Xref - output it
		 * including the taxon ID. If there is not and if the category is "gene", travel
		 * down the subsumption hierarchy and look for a human UniProt ID.
		 */

		int count = 0;
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				if (count++ % 10000 == 0) {
					System.out.println("progress: " + (count - 1));
				}
				OWLClass owlClass = classIterator.next();
				Mapping mapping = retrieveUniProtMapping(ontUtil, owlClass);
				if (mapping != null) {
					outputMapping(writer, mapping);
				}
			}
		}

		ontUtil.close();

	}

	private static void outputMapping(BufferedWriter writer, Mapping mapping) throws IOException {
		writer.write(mapping.getPrId() + "\t" + mapping.getUniprotId() + "\t" + mapping.getTaxonId() + "\n");
	}

	protected static Mapping retrieveUniProtMapping(OntologyUtil ontUtil, OWLClass owlClass) {
		if (owlClass != null) {
			String prId = iriToCurie(owlClass.getIRI().toString());
			if (isGeneLevel(ontUtil, owlClass)) {
				// then walk down the hierarchy and look for a protein annotated as human with a
				// uniprot xref; traverse at most 2 subclass levels
				owlClass = findHumanSubclassWithUniprotXref(ontUtil, owlClass, true);
			}

			return retrieveUniProtMappingDirect(ontUtil, owlClass, prId);
		}
		return null;
	}

	private static Mapping retrieveUniProtMappingDirect(OntologyUtil ontUtil, OWLClass owlClass, String prId) {
		if (owlClass != null) {
			Set<String> dbXrefs = ontUtil.getDbXrefs(owlClass);
			for (String xref : dbXrefs) {
				if (xref.startsWith("UniProtKB:")) {
					String uniprotId = xref;
					String taxonId = getTaxonId(ontUtil, owlClass);
					return new Mapping(prId, uniprotId, taxonId);
				}
			}
		}
		return null;
	}

	private static OWLClass findHumanSubclassWithUniprotXref(OntologyUtil ontUtil, OWLClass owlClass,
			boolean checkChildren) {
		Set<OWLClassExpression> subClasses = owlClass.getSubClasses(ontUtil.getOnt());

		Set<OWLClass> children = new HashSet<OWLClass>();
		for (OWLClassExpression subClass : subClasses) {
			if (subClass instanceof OWLClass) {
				children.add((OWLClass) subClass);
				Mapping mapping = retrieveUniProtMappingDirect(ontUtil, (OWLClass) subClass, null);
				if (mapping != null && mapping.getTaxonId() != null && mapping.getTaxonId().equals(HUMAN_TAXON_CURIE)
						&& mapping.getUniprotId() != null) {
					return (OWLClass) subClass;
				}
			}
		}

		// if no human class with uniprot has been found, check the grandchildren
		if (checkChildren) {
			for (OWLClass child : children) {
				OWLClass grandChild = findHumanSubclassWithUniprotXref(ontUtil, child, false);
				if (grandChild != null) {
					return grandChild;
				}
			}
		}

		return null;

	}

	protected static boolean isGeneLevel(OntologyUtil ontUtil, OWLClass cls) {
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

	protected static boolean hasUniProtXref(OntologyUtil ontUtil, OWLClass owlClass) {
		return retrieveUniProtMapping(ontUtil, owlClass) != null;
	}

	private static String getTaxonId(OntologyUtil ontUtil, OWLClass owlClass) {
		Map<String, Set<String>> outgoingEdges = ontUtil.getOutgoingEdges(owlClass);
		if (outgoingEdges.containsKey(RO_ONLY_IN_TAXON)) {
			return (iriToCurie(outgoingEdges.get(RO_ONLY_IN_TAXON).iterator().next()));
		}
		return null;
	}

	private static String iriToCurie(String iri) {
		String curie = StringUtils.removePrefix(iri, "http://purl.obolibrary.org/obo/");
		curie = curie.replace("_", ":");
		return curie;
	}

	@Data
	protected static class Mapping {
		private final String prId;
		private final String uniprotId;
		private final String taxonId;
	}

}
