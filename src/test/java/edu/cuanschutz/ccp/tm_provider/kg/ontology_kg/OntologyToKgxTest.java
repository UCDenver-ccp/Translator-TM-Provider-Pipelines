package edu.cuanschutz.ccp.tm_provider.kg.ontology_kg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.trapi.pr.CRAFTProteinHasTaxonomyStats;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class OntologyToKgxTest {

	private static final String PR_P49758 = "http://purl.obolibrary.org/obo/PR_P49758";
	private static final String PR_000000001 = "http://purl.obolibrary.org/obo/PR_000000001";

	@Test
	public void testGetRelationToTargetIri() throws OWLOntologyCreationException {
		InputStream owlStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml");
		OntologyUtil ontUtil = new OntologyUtil(owlStream);

		OWLClass cls = ontUtil.getOWLClassFromId(PR_P49758);

		assertNotNull(cls);

		Map<String, Set<String>> relationToTargetIriMap = ontUtil.getOutgoingEdges(cls);

		Map<String, Set<String>> expectedRelationToTargetIriMap = new HashMap<String, Set<String>>();

		expectedRelationToTargetIriMap.put("SUBCLASS_OF", CollectionsUtil.createSet(
				"http://purl.obolibrary.org/obo/PR_000013961", "http://purl.obolibrary.org/obo/PR_000029067"));
		expectedRelationToTargetIriMap.put("http://purl.obolibrary.org/obo/RO_0002160",
				CollectionsUtil.createSet("http://purl.obolibrary.org/obo/NCBITaxon_9606"));
		expectedRelationToTargetIriMap.put("http://purl.obolibrary.org/obo/pr#has_gene_template",
				CollectionsUtil.createSet("http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=10002"));
		
		assertEquals(expectedRelationToTargetIriMap, relationToTargetIriMap);

	}
	
	@Test
	public void testGetDbXrefs() throws OWLOntologyCreationException {
		InputStream owlStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml");
		OntologyUtil ontUtil = new OntologyUtil(owlStream);

		OWLClass cls = ontUtil.getOWLClassFromId(PR_P49758);

		assertNotNull(cls);

		Set<String> xrefs = ontUtil.getDbXrefs(cls);

		Set<String> expectedXrefs= CollectionsUtil.createSet("Reactome:R-HSA-939789","UniProtKB:P49758");		
		assertEquals(expectedXrefs, xrefs);

	}
	
	
	
	@Test
	public void testHasTaxon() throws OWLOntologyCreationException {
		InputStream owlStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(), "sample.owl.xml");
		OntologyUtil ontUtil = new OntologyUtil(owlStream);

		OWLClass cls = ontUtil.getOWLClassFromId(PR_P49758);

		assertNotNull(cls);

		assertTrue(CRAFTProteinHasTaxonomyStats.hasTaxon(cls, ontUtil));
		
		
		cls = ontUtil.getOWLClassFromId(PR_000000001);

		assertNotNull(cls);

		assertFalse(CRAFTProteinHasTaxonomyStats.hasTaxon(cls, ontUtil));
		

	}

}
