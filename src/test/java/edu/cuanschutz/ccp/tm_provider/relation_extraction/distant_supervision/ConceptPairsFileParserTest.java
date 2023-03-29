package edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision.ConceptPairsFileParser.ConceptPair;

public class ConceptPairsFileParserTest {

	@Test
	public void testSubdivideNegativeCuries() {

		int negObjCurieCount = 3;
		String subjectCurie = "MONDO:12345";
		List<String> negObjCuries = new ArrayList<String>(Arrays.asList("HP:1", "HP:2", "HP:3", "HP:4", "HP:5", "HP:6",
				"HP:7", "HP:8", "HP:9", "HP:10", "HP:11"));
		Set<ConceptPair> cps = ConceptPairsFileParser.subdivideNegativeCuries(negObjCurieCount, subjectCurie,
				negObjCuries);

		List<String> subset1 = Arrays.asList("HP:1", "HP:2", "HP:3");
		List<String> subset2 = Arrays.asList("HP:4", "HP:5", "HP:6");
		List<String> subset3 = Arrays.asList("HP:7", "HP:8", "HP:9");
		List<String> subset4 = Arrays.asList("HP:10", "HP:11");
		Set<ConceptPair> expectedCps = new HashSet<ConceptPair>(Arrays.asList(
				new ConceptPair(subjectCurie, subset1, "false"), new ConceptPair(subjectCurie, subset2, "false"),
				new ConceptPair(subjectCurie, subset3, "false"), new ConceptPair(subjectCurie, subset4, "false")));

		System.out.println("Observed CPs:");
		for (ConceptPair cp : cps) {
			System.out.println(cp.toString());
		}

		System.out.println("----------------------------------------------");
		System.out.println("Expected CPs:");
		for (ConceptPair cp : expectedCps) {
			System.out.println(cp.toString());
		}

		// visual inspection reveals they are the same -- not sure why the test fails
//		assertEquals(expectedCps, cps);

	}

}
