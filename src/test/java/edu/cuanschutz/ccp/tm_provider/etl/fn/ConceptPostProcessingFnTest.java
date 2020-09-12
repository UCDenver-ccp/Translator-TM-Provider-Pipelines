package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ConceptPostProcessingFnTest {

	@Test
	public void testPromoteAnnots() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation annot1 = factory.createAnnotation(25, 35, "some text", "PR:00000000022");
		TextAnnotation annot2 = factory.createAnnotation(25, 35, "some text", "PR:00000000000");
		TextAnnotation annot3 = factory.createAnnotation(0, 5, "some text", "PR:00000000025");
		TextAnnotation annot4 = factory.createAnnotation(0, 5, "some text", "PR:00000000020");

		Set<TextAnnotation> annots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4));

		Map<String, String> promotionMap = new HashMap<String, String>();
		promotionMap.put("PR:00000000025", "PR:00000000020");

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn.promoteAnnots(annots, promotionMap);

		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot4));

		assertEquals(expectedOutputAnnots.size(), outputAnnots.size());
		assertEquals(expectedOutputAnnots, outputAnnots);
	}

	

	@Test
	public void testConvertExtensionToObo() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation annot1 = factory.createAnnotation(25, 35, "some text", "PR_EXT:00000000022");
		TextAnnotation annot2 = factory.createAnnotation(25, 35, "some text", "PR:some_extension_cls");
		TextAnnotation annot3 = factory.createAnnotation(0, 5, "some text", "PR:00000000025");
		TextAnnotation annot4 = factory.createAnnotation(0, 5, "some text", "PR:00000000020");

		Set<TextAnnotation> annots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4));
		Map<String, Set<String>> extensionToOboMap = new HashMap<String, Set<String>>();
		extensionToOboMap.put("PR_EXT:00000000022", CollectionsUtil.createSet("PR:00000000022"));
		extensionToOboMap.put("PR:some_extension_cls", CollectionsUtil.createSet("PR:00000000123", "PR:00000000456"));

		Set<TextAnnotation> outputAnnots = ConceptPostProcessingFn.convertExtensionToObo(annots, extensionToOboMap);

		TextAnnotation annot1Updated = factory.createAnnotation(25, 35, "some text", "PR:00000000022");
		TextAnnotation annot2aUpdated = factory.createAnnotation(25, 35, "some text", "PR:00000000123");
		TextAnnotation annot2bUpdated = factory.createAnnotation(25, 35, "some text", "PR:00000000456");

		Set<TextAnnotation> expectedOutputAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(annot1Updated, annot2aUpdated, annot2bUpdated, annot3, annot4));

		assertEquals(expectedOutputAnnots.size(), outputAnnots.size());
		assertEquals(expectedOutputAnnots, outputAnnots);

	}

}
