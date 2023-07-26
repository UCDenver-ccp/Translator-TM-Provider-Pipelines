package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AddAxiom;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotation;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.PrefixManager;
import org.semanticweb.owlapi.util.DefaultPrefixManager;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;

/**
 * Creates the subclass hierarchy of UMLS concepts mined from "isa" relations
 * from the Metathesaurus MRREL.RRF file
 *
 */
public class CreateUmlsSubclassOntology {

	public static final String ONTOLOGY_IRI = "http://umls/isa";

	/**
	 * @param mrrelIsaFile - this is a derivative of the MRREL.RRF file that comes
	 *                     with the UMLS Metathesaurus distribution.
	 * 
	 *                     <pre>
	 * gunzip -c MRREL.RRF.gz| awk -F'|' '$8 == "isa"' > isa.out
	 *                     </pre>
	 *
	 * @param outputFile   - the OWL file to use where the generated ontology will
	 *                     be stored.
	 * @throws OWLOntologyCreationException
	 * @throws OWLOntologyStorageException
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void buildOntology(File mrrelIsaFile, File cuiToNameFile, File outputFile)
			throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException, IOException {

		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ont = manager.createOntology();
		OWLDataFactory df = ont.getOWLOntologyManager().getOWLDataFactory();
		IRI ontologyIRI = IRI.create(ONTOLOGY_IRI);
		PrefixManager pm = new DefaultPrefixManager(ontologyIRI.toString());

		System.out.println("Adding subclass axioms");
		addSubclassAxioms(mrrelIsaFile, manager, ont, df, pm);

		System.out.println("Adding labels");
		addLabels(cuiToNameFile, manager, ont, df);

		manager.saveOntology(ont, IRI.create(outputFile.toURI()));

	}

	private static void addSubclassAxioms(File mrrelIsaFile, OWLOntologyManager manager, OWLOntology ont,
			OWLDataFactory df, PrefixManager pm) throws FileNotFoundException, IOException {
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(mrrelIsaFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split("\\|");

			String parentCui = cols[0];
			String childCui = cols[4];

			addSubclassAxiom(childCui, parentCui, manager, ont, df, pm);
		}
	}

	private static void addSubclassAxiom(String childCui, String parentCui, OWLOntologyManager manager, OWLOntology ont,
			OWLDataFactory df, PrefixManager pm) {
		OWLClass childClass = df.getOWLClass("/" + childCui, pm);
		OWLClass parentClass = df.getOWLClass("/" + parentCui, pm);
		OWLSubClassOfAxiom axiom = df.getOWLSubClassOfAxiom(childClass, parentClass);
		manager.addAxiom(ont, axiom);
	}

	private static void addLabels(File cuiToNameFile, OWLOntologyManager manager, OWLOntology ont, OWLDataFactory df)
			throws FileNotFoundException, IOException {
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(cuiToNameFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}

			String s = line.getText();
			// file is comma delimited with quotes around fields
			int firstComma = s.indexOf(",");
			String cui = s.substring(0, firstComma);
			String label = s.substring(firstComma + 1);

			// remove quotes
			cui = StringUtil.removePrefix(cui, "\"");
			cui = StringUtil.removeSuffix(cui, "\"");

			label = StringUtil.removePrefix(label, "\"");
			label = StringUtil.removeSuffix(label, "\"");

			addLabel(cui, label, manager, ont, df);
		}
	}

	private static void addLabel(String cui, String label, OWLOntologyManager manager, OWLOntology ont,
			OWLDataFactory df) {

		IRI iri = IRI.create(ONTOLOGY_IRI + "/" + cui);
		OWLAnnotation labelAnno = df.getOWLAnnotation(df.getRDFSLabel(), df.getOWLLiteral(label, "en"));
		OWLAxiom axiom = df.getOWLAnnotationAssertionAxiom(iri, labelAnno);
		manager.applyChange(new AddAxiom(ont, axiom));
	}

	public static void main(String[] args) {

		File mrrelIsaFile = new File(
				"/Users/bill/projects/ncats-translator/integrating-semmed/umls/2023AA/META/isa.out.gz");
		File cuiToLabelFile = new File(
				"/Users/bill/projects/ncats-translator/integrating-semmed/umls/semmed_cui_to_name.csv.gz");
		File outputOwlFile = new File("/Users/bill/projects/ncats-translator/integrating-semmed/umls/umls_isa.owl");

		try {
			buildOntology(mrrelIsaFile, cuiToLabelFile, outputOwlFile);
		} catch (OWLOntologyCreationException | OWLOntologyStorageException | IOException e) {
			e.printStackTrace();
		}

//		OWLOntologyManager man = OWLManager.createOWLOntologyManager();
//		OWLOntology o;
//
//		File dir = new File("/tmp");
//
//		try {
//			o = man.createOntology();
//			System.out.println(o);
//
//			OWLDataFactory df = o.getOWLOntologyManager().getOWLDataFactory();
//
//			IRI ontologyIRI = IRI.create("http://owl.api.tutorial");
////			PrefixManager pm = new DefaultPrefixManager();
//			PrefixManager pm = new DefaultPrefixManager(ontologyIRI.toString());
//			OWLClass person = df.getOWLClass("/Person", pm);
//			OWLClass woman = df.getOWLClass("/Woman", pm);
//			OWLSubClassOfAxiom w_sub_p = df.getOWLSubClassOfAxiom(woman, person);
//			man.addAxiom(o, w_sub_p);
//
//			OWLAnnotation labelAnno = df.getOWLAnnotation(df.getRDFSLabel(), df.getOWLLiteral("woman", "en"));
//			OWLAxiom ax1 = df.getOWLAnnotationAssertionAxiom(woman.getIRI(), labelAnno);
//			man.applyChange(new AddAxiom(o, ax1));
//
//			File file = new File(dir, "umls.owl");
//			man.saveOntology(o, IRI.create(file.toURI()));
//		} catch (OWLOntologyCreationException e) {
//			e.printStackTrace();
//		} catch (OWLOntologyStorageException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
