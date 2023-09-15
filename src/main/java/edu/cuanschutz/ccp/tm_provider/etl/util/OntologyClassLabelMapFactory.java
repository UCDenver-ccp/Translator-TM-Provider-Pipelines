package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import lombok.Data;

/**
 * Creates a mapping from ontology classes to their corresponding label.
 *
 */
@Data
public class OntologyClassLabelMapFactory {

	public void createMappingFile(OntologyUtil ontUtil, BufferedWriter writer) throws IOException {

		int count = 0;
		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			if (count++ % 10000 == 0) {
				System.out.println(String.format("progress: %d...", count - 1));
			}
			OWLClass cls = classIterator.next();

			String label = ontUtil.getLabel(cls);

			if (label == null) {
				label = "_null";
			}

			if (label.endsWith("\"")) {
				label = label.substring(0, label.length() - 1);
			}

			writeMapping(writer, getId(cls), label);

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

		File ontologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies");
		File craftOntologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/craft");
		File outputFile = new File(ontologyDir, "ontology-class-label-map.tsv");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {

			for (Iterator<File> fileIterator = FileUtil.getFileIterator(craftOntologyDir, false,
					".obo.gz"); fileIterator.hasNext();) {
				File ontologyFile = fileIterator.next();
				System.out.println("Processing " + ontologyFile.getName());
				OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));
				new OntologyClassLabelMapFactory().createMappingFile(ontUtil, writer);
			}

			for (Iterator<File> fileIterator = FileUtil.getFileIterator(ontologyDir, false, ".owl.gz"); fileIterator
					.hasNext();) {
				File ontologyFile = fileIterator.next();

				System.out.println("Processing " + ontologyFile.getName());
				OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));
				new OntologyClassLabelMapFactory().createMappingFile(ontUtil, writer);
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

}
