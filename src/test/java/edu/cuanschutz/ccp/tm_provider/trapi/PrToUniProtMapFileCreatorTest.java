package edu.cuanschutz.ccp.tm_provider.trapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.trapi.PrToUniProtMapFileCreator.Mapping;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class PrToUniProtMapFileCreatorTest {

	@Test
	public void testRetrieveUniprotMappingFromClassWithXref() throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));

		OWLClass owlClassWithUniProtXref = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_P49758");

		Mapping mapping = PrToUniProtMapFileCreator.retrieveUniProtMapping(ontUtil, owlClassWithUniProtXref);

		assertEquals("PR:P49758", mapping.getPrId());
		assertEquals("UniProtKB:P49758", mapping.getUniprotId());
		assertEquals("NCBITaxon:9606", mapping.getTaxonId());

	}

	@Test
	public void testRetrieveUniprotMappingFromClassWithGeneCategory() throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));

		OWLClass owlClassWithUniProtXref = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_000013961");

		Mapping mapping = PrToUniProtMapFileCreator.retrieveUniProtMapping(ontUtil, owlClassWithUniProtXref);

		assertEquals("PR:000013961", mapping.getPrId());
		assertEquals("UniProtKB:P49758", mapping.getUniprotId());
		assertEquals("NCBITaxon:9606", mapping.getTaxonId());

	}

	@Test
	public void testRetrieveUniprotMappingFromClassWithoutGeneCategory() throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));

		OWLClass owlClassWithUniProtXref = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_000036194");

		Mapping mapping = PrToUniProtMapFileCreator.retrieveUniProtMapping(ontUtil, owlClassWithUniProtXref);

		assertNull(mapping);

	}

	@Test
	public void testRetrieveUniprotMappingFromClassWithGeneCategory2Levels() throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));

		OWLClass owlClassWithUniProtXref = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_000012345");

		assertNotNull(owlClassWithUniProtXref);

		Mapping mapping = PrToUniProtMapFileCreator.retrieveUniProtMapping(ontUtil, owlClassWithUniProtXref);

		assertEquals("PR:000012345", mapping.getPrId());
		assertEquals("UniProtKB:P01234", mapping.getUniprotId());
		assertEquals("NCBITaxon:9606", mapping.getTaxonId());

	}

	@Test
	public void testCftrIsGeneLevel() throws OWLOntologyCreationException, IOException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));
		try {
			OWLClass cftrAtGeneLevel = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_000001044");
			assertTrue(PrToUniProtMapFileCreator.isGeneLevel(ontUtil, cftrAtGeneLevel));
		} finally {
			ontUtil.close();
		}
	}

	/**
	 * 
	 * Our data set has mapped to the species-agnostic (gene-level) concept for
	 * CFTR: PR:000001044 We should return the UniProt identifier for human CFTR:
	 * UniProtKB:P13569
	 */
	@Test
	public void testRetrieveUniprotMappingForCFTR() throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(
				ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml"));

		OWLClass cftrAtGeneLevel = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/PR_000001044");

		assertNotNull(cftrAtGeneLevel);

		Mapping mapping = PrToUniProtMapFileCreator.retrieveUniProtMapping(ontUtil, cftrAtGeneLevel);

		assertEquals("PR:000001044", mapping.getPrId());
		assertEquals("UniProtKB:P13569", mapping.getUniprotId());
		assertEquals("NCBITaxon:9606", mapping.getTaxonId());

	}

}
