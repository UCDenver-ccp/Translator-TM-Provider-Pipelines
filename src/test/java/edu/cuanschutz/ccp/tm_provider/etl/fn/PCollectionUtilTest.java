package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class PCollectionUtilTest {

	@Test
	public void testGetKV() {

		String input = "CHEBI_SO_EXT:threonine\tCHEBI:26986\tCHEBI:32835";

		KV<String, Set<String>> kv = PCollectionUtil.getKV(Delimiter.TAB, Delimiter.TAB, input);

		KV<String, Set<String>> expectedKv = KV.of("CHEBI_SO_EXT:threonine",
				CollectionsUtil.createSet("CHEBI:26986", "CHEBI:32835"));

		assertEquals(expectedKv, kv);

	}

}
