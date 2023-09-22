package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * This code was originally designed to migrate the GO_MF concepts that were
 * automatically turned into extension classes back to normal GO classes, and
 * add them to the existing GO_MF class Bionlp files. This is in order to
 * include GO_MF in the concept recognition evaluation platform.
 *
 */
public class CraftGoMfExtReverter {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	public static void revertExt(File goOwlFile, File origGoMfBionlpDir, File extGoMfBionlpDir, File craftBaseDir)
			throws OWLOntologyCreationException, IOException {

		System.out.println("Loading go.owl...");
		OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(goOwlFile)));

		System.out.println("Processing GO_MF...");
		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		for (Iterator<File> fileIter = FileUtil.getFileIterator(origGoMfBionlpDir, false); fileIter.hasNext();) {
			File origBionlpFile = fileIter.next();
			File extBionlpFile = new File(extGoMfBionlpDir, origBionlpFile.getName());

			TextDocument origTd = loadBionlpFile(craftBaseDir, bionlpReader, origBionlpFile);
			TextDocument extTd = loadBionlpFile(craftBaseDir, bionlpReader, extBionlpFile);

			System.out.println(
					String.format("%s Before count: %d", origBionlpFile.getName(), origTd.getAnnotations().size()));

			for (TextAnnotation extAnnot : extTd.getAnnotations()) {
				String conceptId = extAnnot.getClassMention().getMentionName();
				if (conceptId.contains("GO_EXT:")) {
					conceptId = conceptId.replace("_EXT", "");
					extAnnot.getClassMention().setMentionName(conceptId);
				}
				conceptId = OBO_PURL + conceptId.replace(":", "_");

				OWLClass cls = ontUtil.getOWLClassFromId(conceptId);
				if (cls != null) {
					System.out.println("Adding annot for cls: " + cls.getIRI().toString());
					origTd.addAnnotation(extAnnot);
				}
			}

			System.out.println(
					String.format("%s After count: %d", origBionlpFile.getName(), origTd.getAnnotations().size()));
			// overwrite the original bionlp file with the updated GO_MF annotations
			BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
			bionlpWriter.serialize(origTd, origBionlpFile, ENCODING);

		}

	}

	private static TextDocument loadBionlpFile(File craftBaseDir, BioNLPDocumentReader bionlpReader,
			File origBionlpFile) throws IOException {
		String docId = origBionlpFile.getName().split("\\.")[0];
		File txtFile = ExcludeCraftNestedConcepts.getTextFile(docId, craftBaseDir);
		return bionlpReader.readDocument(docId, "craft", origBionlpFile, txtFile, ENCODING);
	}

	public static void main(String[] args) {
		File craftBaseDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git");

		File origGoMfBionlpDir = new File(
				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/bionlp-original/go_mf");
		File extGoMfBionlpDir = new File(
				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/bionlp-original/go_mf_ext");

		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");
		File goOwlFile = new File(ontBase, "go.owl.gz");

		try {
			revertExt(goOwlFile, origGoMfBionlpDir, extGoMfBionlpDir, craftBaseDir);
		} catch (OWLOntologyCreationException | IOException e) {
			e.printStackTrace();
		}

	}

}
