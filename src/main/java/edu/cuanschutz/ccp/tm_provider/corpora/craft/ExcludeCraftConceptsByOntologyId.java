package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.oger.dict.ChebiOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.ClOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.GoBpOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.GoCcOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.GoMfOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.MondoOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.NcbiTaxonOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.PrOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.SoOgerDictFileFactory;
import edu.cuanschutz.ccp.tm_provider.oger.dict.UberonOgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * Our OGER dictionaries purposely exclude some ontology concepts. This class
 * was designed to remove those concepts from the CRAFT gold standard so that
 * they are not part of the evaluation. The concepts that are excluded can be
 * found as part of the OGER dictionary factories.
 * 
 * We currently only exclude classes for CHEBI and MONDO
 *
 */
public class ExcludeCraftConceptsByOntologyId {
	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public static void excludeClasses(File ontologyFile, Collection<String> rootIrisToExclude,
			Collection<String> individualIrisToExclude, File bionlpDir, File craftBaseDir)
			throws OWLOntologyCreationException, IOException {

		Set<OWLClass> excludedClasses = new HashSet<OWLClass>();

		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);
		// add the individually excluded classes
		for (String iri : individualIrisToExclude) {
			OWLClass cls = ontUtil.getOWLClassFromId(iri);
			if (cls != null) {
				excludedClasses.add(cls);
			} else {
				throw new IllegalArgumentException("Unknown class: " + iri);
			}
		}

		// now add the "root" classes and all descendents of root classes
		for (String iri : rootIrisToExclude) {
			OWLClass cls = ontUtil.getOWLClassFromId(iri);
			if (cls != null) {
				excludedClasses.add(cls);
				for (OWLClass child : ontUtil.getDescendents(cls)) {
					excludedClasses.add(child);
				}
			} else {
				throw new IllegalArgumentException("Unknown class: " + iri);
			}
		}

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		for (Iterator<File> fileIter = FileUtil.getFileIterator(bionlpDir, true, "bionlp"); fileIter.hasNext();) {
			File file = fileIter.next();
			String docId = file.getName().split("\\.")[0];
			File txtFile = ExcludeCraftNestedConcepts.getTextFile(docId, craftBaseDir);
			TextDocument td = bionlpReader.readDocument(docId, "craft", file, txtFile, ENCODING);

			Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();
			for (TextAnnotation annot : td.getAnnotations()) {
				OWLClass cls = getOwlClass(ontUtil, annot);
				if (cls != null) {
					if (excludedClasses.contains(cls)) {
						toRemove.add(annot);
					}
				} else {
//					System.err.println("null class for " + annot.getClassMention().getMentionName());
				}
			}

			for (TextAnnotation remove : toRemove) {
				td.getAnnotations().remove(remove);
				OWLClass cls = getOwlClass(ontUtil, remove);
				System.out.println("Removing: " + remove.getClassMention().getMentionName() + " "
						+ ontUtil.getLabel(cls) + " from " + file.getAbsolutePath());
			}

			if (toRemove.size() > 0) {
				BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
				bionlpWriter.serialize(td, file, ENCODING);
			}

		}

	}

	private static OWLClass getOwlClass(OntologyUtil ontUtil, TextAnnotation annot) {

		String iri = annot.getClassMention().getMentionName();
		if (!iri.startsWith(OBO_PURL)) {
			iri = iri.replace(":", "_");
			iri = OBO_PURL + iri;
		}
		OWLClass cls = ontUtil.getOWLClassFromId(iri);
		return cls;
	}

	public static void main(String[] args) {
		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");
		File chebiOwlFile = new File(ontBase, "chebi.owl");
		File clOwlFile = new File(ontBase, "cl.owl");
		File goOwlFile = new File(ontBase, "go.owl");
		File prOwlFile = new File(ontBase, "pr.owl");
		File soOwlFile = new File(ontBase, "so.owl");
		File uberonOwlFile = new File(ontBase, "uberon.owl");
		File mondoOwlFile = new File(ontBase, "mondo.owl");
		File ncbiTaxonOwlFile = new File(ontBase, "ncbitaxon.owl");

		File craftBaseDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git");
//		File bionlpBaseDir = new File(
//				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/bionlp-no-nested");
		File bionlpBaseDir = new File(
				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/bionlp-exclude-specific");

		try {
			System.out.println("CHEBI...");
			excludeClasses(chebiOwlFile, ChebiOgerDictFileFactory.EXCLUDED_ROOT_CLASSES,
					ChebiOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

			System.out.println("MONDO...");
			excludeClasses(mondoOwlFile, MondoOgerDictFileFactory.EXCLUDED_ROOT_CLASSES,
					MondoOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

			System.out.println("NCBITaxon...");
			excludeClasses(ncbiTaxonOwlFile, NcbiTaxonOgerDictFileFactory.EXCLUDED_ROOT_CLASSES,
					NcbiTaxonOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

			System.out.println("CL...");
			excludeClasses(clOwlFile, Collections.emptyList(), ClOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES,
					bionlpBaseDir, craftBaseDir);

			System.out.println("GO_BP...");
			excludeClasses(goOwlFile, Collections.emptyList(), GoBpOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES,
					bionlpBaseDir, craftBaseDir);

//			System.out.println("GO_CC...");
//			excludeClasses(goOwlFile, Collections.emptyList(),
//					GoCcOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);
//			
//			System.out.println("GO_MF...");
//			excludeClasses(goOwlFile, Collections.emptyList(),
//					GoMfOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

			System.out.println("PR...");
			excludeClasses(prOwlFile, Collections.emptyList(), PrOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES,
					bionlpBaseDir, craftBaseDir);

			System.out.println("SO...");
			excludeClasses(soOwlFile, SoOgerDictFileFactory.EXCLUDED_ROOT_CLASSES,
					SoOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

			System.out.println("UBERON...");
			excludeClasses(uberonOwlFile, UberonOgerDictFileFactory.EXCLUDED_ROOT_CLASSES,
					UberonOgerDictFileFactory.EXCLUDED_INDIVIDUAL_CLASSES, bionlpBaseDir, craftBaseDir);

		} catch (OWLOntologyCreationException | IOException e) {
			e.printStackTrace();
		}

	}

}
