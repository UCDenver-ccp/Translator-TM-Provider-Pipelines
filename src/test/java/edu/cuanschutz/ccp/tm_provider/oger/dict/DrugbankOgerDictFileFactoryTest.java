package edu.cuanschutz.ccp.tm_provider.oger.dict;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class DrugbankOgerDictFileFactoryTest {

	@Test
	public void testAugmentSynonyms() {

		String iri = "http://iri";
		Set<String> syns = new HashSet<String>(Arrays.asList("Thyroid extract, porcine"));
		OntologyUtil ontUtil = null;
		Set<String> augmentedSyns = new DrugbankOgerDictFileFactory().augmentSynonyms(iri, syns, ontUtil);
		assertEquals(0, augmentedSyns.size());

	}

}
