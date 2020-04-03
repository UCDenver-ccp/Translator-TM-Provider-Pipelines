package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class AncestorTableFileFactory {

	public static void createAncestorFile(File ontologyFile, File ancestorTableFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(ancestorTableFile)) {
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				OWLClass cls = classIterator.next();

				if (cls.getIRI().toString().contains("DOID")) {
					Set<OWLClass> ancestors = ontUtil.getAncestors(cls);

					writer.write(String.format("%s\t%s\n", getId(cls), getId(cls)));
					for (OWLClass ancestor : ancestors) {
						writer.write(String.format("%s\t%s\n", getId(cls), getId(ancestor)));
					}

				}
			}
		}
	}

	private static Object getId(OWLClass cls) {
		String id = StringUtil.removePrefix(cls.getIRI().toString(), "http://purl.obolibrary.org/obo/");
		id = id.replace("_", ":");
		return id;
	}

	public static void createAncestorFileForHGNC(File hgncDownloadFile, File ancestorTableFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(ancestorTableFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(hgncDownloadFile, CharacterEncoding.UTF_8,
					"HGNC ID"); lineIter.hasNext();) {
				String line = lineIter.next().getText();
				String[] cols = line.split("\\t");

				// HGNC ID
				// Status
				// Approved symbol
				// Approved name
				// Alias name
				// Alias symbol

				String hgncId = cols[0];

				String s = String.format("%s\t%s\n", hgncId, "gene");
				if (!alreadyWritten.contains(s)) {
					writer.write(s);
					alreadyWritten.add(s);
				}
			}

		}

	}

	public static void main(String[] args) {
		File ontologyFile = new File(
				"/Users/bill/projects/ncats-translator/prototype/oger-docker.git/oger-craft-resources/ontologies/doid.owl");
		File ancestorTableFile = new File(
				"/Users/bill/projects/ncats-translator/prototype/testing-data/asthma/bigquery/DOID.ancestor.tsv");

		try {
			createAncestorFile(ontologyFile, ancestorTableFile);
		} catch (OWLOntologyCreationException | IOException e) {
			e.printStackTrace();
		}

//		File hgncDownloadFile = new File(
//				"/Users/bill/projects/ncats-translator/prototype/oger-docker.git/oger-craft-resources/ontologies/hgnc_download.tsv");
//		File ancestorTableFile = new File(
//				"/Users/bill/projects/ncats-translator/prototype/testing-data/asthma/bigquery/HGNC.ancestor.tsv");
//
//		try {
//			createAncestorFileForHGNC(hgncDownloadFile, ancestorTableFile);
//		} catch (OWLOntologyCreationException | IOException e) {
//			e.printStackTrace();
//		}
	}

}
