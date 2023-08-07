package edu.cuanschutz.ccp.tm_provider.oger.dict;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.kg.ontology_kg.OntologyToKgxTest;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class PrOgerDictFileFactoryTest {

	@Test
	public void testAddGeneSynonyms() throws OWLOntologyCreationException {

		InputStream owlStream = ClassPathUtil.getResourceStreamFromClasspath(OntologyToKgxTest.class, "sample.owl.xml");
		OntologyUtil ontUtil = new OntologyUtil(owlStream);

		String iri = "http://purl.obolibrary.org/obo/PR_P49758";

		Map<String, Set<String>> geneIdToSynMap = new HashMap<String, Set<String>>();

		geneIdToSynMap.put("HGNC:10002", new HashSet<String>(Arrays.asList("syn1", "syn2")));
		geneIdToSynMap.put("MGI:11234", new HashSet<String>(Arrays.asList("syn3", "syn4")));

		Set<String> synonyms = PrOgerDictFileFactory.addGeneSynonyms(iri, new HashSet<String>(Arrays.asList("syn0")),
				ontUtil, geneIdToSynMap);

		Set<String> expectedSynonyms = new HashSet<String>(Arrays.asList("syn0", "syn1", "syn2"));

		assertEquals(expectedSynonyms, synonyms);

	}

}
