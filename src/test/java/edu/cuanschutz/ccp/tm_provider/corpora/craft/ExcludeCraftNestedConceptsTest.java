package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.corpora.craft.ExcludeCraftNestedConcepts.ExcludeExactOverlaps;
import edu.cuanschutz.ccp.tm_provider.corpora.craft.ExcludeCraftNestedConcepts.Ont;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ExcludeCraftNestedConceptsTest {

	private static final String DOC_ID = "doc:1";
	private static final TextAnnotationFactory FACTORY = TextAnnotationFactory.createFactoryWithDefaults(DOC_ID);

	@Test
	public void testEncompasses() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		// 01234567890123456789
		TextAnnotation rbcAnnot = factory.createAnnotation(0, 14, "red blood cell", "CL:12345");
		TextAnnotation bloodAnnot = factory.createAnnotation(4, 9, "blood", "UBERON:12345");

		assertTrue(ExcludeCraftNestedConcepts.encompasses(rbcAnnot, bloodAnnot));
		assertFalse(ExcludeCraftNestedConcepts.encompasses(bloodAnnot, rbcAnnot));

		TextAnnotation annot1 = factory.createAnnotation(0, 14, "", "CL:12345");
		TextAnnotation annot2 = factory.createAnnotation(9, 16, "", "UBERON:12345");

		assertFalse(ExcludeCraftNestedConcepts.encompasses(annot1, annot2));

		TextAnnotation redAnnot = factory.createAnnotation(0, 4, "red", "COLOR:red");
		assertTrue(ExcludeCraftNestedConcepts.encompasses(rbcAnnot, redAnnot));
		assertFalse(ExcludeCraftNestedConcepts.encompasses(redAnnot, rbcAnnot));
	}

	@Test
	public void testGetOverlappingAnnotSets() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextAnnotation annot1 = factory.createAnnotation(0, 14, "", "1");
		annot1.setAnnotationID("annot1");
		TextAnnotation annot2 = factory.createAnnotation(0, 9, "", "2");
		annot2.setAnnotationID("annot2");
		TextAnnotation annot3 = factory.createAnnotation(15, 25, "", "3");
		annot3.setAnnotationID("annot3");
		TextAnnotation annot4 = factory.createAnnotation(33, 40, "", "4");
		annot4.setAnnotationID("annot4");
		TextAnnotation annot5 = factory.createAnnotation(35, 42, "", "5");
		annot5.setAnnotationID("annot5");
		TextAnnotation annot6 = factory.createAnnotation(35, 55, "", "6");
		annot6.setAnnotationID("annot6");
		TextAnnotation annot7 = factory.createAnnotation(65, 75, "", "7");
		annot7.setAnnotationID("annot7");

		// the annotations should get sorted so input order should not matter
		List<TextAnnotation> annotations = Arrays.asList(annot7, annot3, annot1, annot2, annot5, annot6, annot4);
		List<Set<TextAnnotation>> overlappingAnnotSets = ExcludeCraftNestedConcepts
				.getOverlappingAnnotSets(annotations);

		List<Set<TextAnnotation>> expectedOverlappingAnnotSets = new ArrayList<Set<TextAnnotation>>();
		expectedOverlappingAnnotSets.add(CollectionsUtil.createSet(annot1, annot2));
		expectedOverlappingAnnotSets.add(CollectionsUtil.createSet(annot4, annot5, annot6));

		assertEquals(expectedOverlappingAnnotSets.size(), overlappingAnnotSets.size());
		assertEquals(expectedOverlappingAnnotSets, overlappingAnnotSets);
	}

	@Test
	public void testRemoveAnnotations() {

		Map<Ont, TextDocument> ontToDocMap = getOntToDocMap();

		// these 3 annotations should be removed b/c they are encompassed by other
		// annotations
		Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();
		TextAnnotation annot2_toRemove = getAnnot2();
		annot2_toRemove.setAnnotationID("UBERON|99");
		TextAnnotation annot6_toRemove = getAnnot6();
		annot6_toRemove.setAnnotationID("UBERON|98");
		TextAnnotation annot7_toRemove = getAnnot7();
		annot7_toRemove.setAnnotationID("UBERON|97");
		toRemove.add(annot2_toRemove);
		toRemove.add(annot6_toRemove);
		toRemove.add(annot7_toRemove);

		ExcludeCraftNestedConcepts.removeAnnotations(toRemove, ontToDocMap);

		TextDocument updatedUberonDoc = new TextDocument(DOC_ID, "unknown", getDocText());
		updatedUberonDoc.addAnnotation(getAnnot4());
		updatedUberonDoc.addAnnotation(getAnnot5());
		updatedUberonDoc.addAnnotation(getAnnot9());

		Map<Ont, TextDocument> expectedOntToDocMap = new HashMap<Ont, TextDocument>();
		expectedOntToDocMap.put(Ont.CL, getClDoc());
		expectedOntToDocMap.put(Ont.CHEBI, getChebiDoc());
		expectedOntToDocMap.put(Ont.UBERON, updatedUberonDoc);

		// check annotation counts first
		assertEquals(expectedOntToDocMap.get(Ont.CL).getAnnotations().size(),
				ontToDocMap.get(Ont.CL).getAnnotations().size());
		assertEquals(expectedOntToDocMap.get(Ont.CHEBI).getAnnotations().size(),
				ontToDocMap.get(Ont.CHEBI).getAnnotations().size());
		assertEquals(expectedOntToDocMap.get(Ont.UBERON).getAnnotations().size(),
				ontToDocMap.get(Ont.UBERON).getAnnotations().size());

		// then check the actual annotations
		assertEquals(expectedOntToDocMap.get(Ont.CL), ontToDocMap.get(Ont.CL));
		assertEquals(expectedOntToDocMap.get(Ont.CHEBI), ontToDocMap.get(Ont.CHEBI));
		assertEquals(expectedOntToDocMap.get(Ont.UBERON), ontToDocMap.get(Ont.UBERON));

	}

	private Map<Ont, TextDocument> getOntToDocMap() {
		Map<Ont, TextDocument> ontToDocMap = new HashMap<Ont, TextDocument>();
		ontToDocMap.put(Ont.CL, getClDoc());
		ontToDocMap.put(Ont.CHEBI, getChebiDoc());
		ontToDocMap.put(Ont.UBERON, getUberonDoc());
		return ontToDocMap;
	}

	private TextDocument getChebiDoc() {
		TextDocument chebiDoc = new TextDocument(DOC_ID, "unknown", getDocText());
		chebiDoc.addAnnotation(getAnnot3());
		chebiDoc.addAnnotation(getAnnot8());
		return chebiDoc;
	}

	private TextDocument getUberonDoc() {
		TextDocument uberonDoc = new TextDocument(DOC_ID, "unknown", getDocText());
		uberonDoc.addAnnotation(getAnnot2());
		uberonDoc.addAnnotation(getAnnot4());
		uberonDoc.addAnnotation(getAnnot5());
		uberonDoc.addAnnotation(getAnnot6());
		uberonDoc.addAnnotation(getAnnot7());
		uberonDoc.addAnnotation(getAnnot9());
		return uberonDoc;
	}

	private TextDocument getClDoc() {
		TextDocument clDoc = new TextDocument(DOC_ID, "unknown", getDocText());
		clDoc.addAnnotation(getAnnot1());
		clDoc.addAnnotation(getAnnot10());
		return clDoc;
	}

	private String getDocText() {
		// @formatter:off
		//              012345678901234567890123456789012345678901234567890123456789
		String text1 = "Red blood cells carry oxygen in the blood.";
		// annot1 = Red blood cells 0..15
		// annot2 = blood 4..9
		// annot3 = oxygen 22..28
		// annot4 = blood 36..41
		
		//              01234567890123456789012345678901234567890123456789012345678901234567890123456789
		String text2 = "The blood brain barrier allows oxygen to pass from the blood to the neurons.";
		// annot5 = blood brain barrier 4..23
		// annot6 = blood 4..9
		// annot7 = brain 10..15
		// annot8 = oxygen 31..37
		// annot9 = blood 55.60
		// annot10 = neurons 68..75
		
		// @formatter:on

		String docText = text1 + " " + text2;
		return docText;
	}

	private TextAnnotation getAnnot10() {
		int offset = 43;
		return FACTORY.createAnnotation(68 + offset, 75 + offset, "neurons", "CL:neuron");
	}

	private TextAnnotation getAnnot9() {
		int offset = 43;
		return FACTORY.createAnnotation(55 + offset, 60 + offset, "blood", "UBERON:blood");
	}

	private TextAnnotation getAnnot8() {
		int offset = 43;
		return FACTORY.createAnnotation(31 + offset, 37 + offset, "oxygen", "CHEBI:oxygen");
	}

	private TextAnnotation getAnnot7() {
		int offset = 43;
		return FACTORY.createAnnotation(10 + offset, 15 + offset, "brain", "UBERON:brain");
	}

	private TextAnnotation getAnnot6() {
		int offset = 43;
		return FACTORY.createAnnotation(4 + offset, 9 + offset, "blood", "UBERON:blood");
	}

	private TextAnnotation getAnnot5() {
		int offset = 43;
		return FACTORY.createAnnotation(4 + offset, 23 + offset, "blood brain barrier", "UBERON:blood_brain_barrier");
	}

	private TextAnnotation getAnnot4() {
		return FACTORY.createAnnotation(36, 41, "blood", "UBERON:blood");
	}

	private TextAnnotation getAnnot3() {
		return FACTORY.createAnnotation(22, 28, "oxygen", "CHEBI:oxygen");
	}

	private TextAnnotation getAnnot2() {
		return FACTORY.createAnnotation(4, 9, "blood", "UBERON:blood");
	}

	private TextAnnotation getAnnot1() {
		return FACTORY.createAnnotation(0, 15, "Red blood cells", "CL:rbc");
	}

	@Test
	public void testidentifyNestedAnnotations() throws FileNotFoundException, IOException {
		BufferedWriter logWriter = null;

		Set<TextAnnotation> overlappingSet = new HashSet<TextAnnotation>();
		overlappingSet.add(getAnnot5());
		overlappingSet.add(getAnnot6());
		overlappingSet.add(getAnnot7());

		Set<TextAnnotation> nestedAnnotations = ExcludeCraftNestedConcepts.identifyNestedAnnotations(overlappingSet,
				logWriter, ExcludeExactOverlaps.NO);

		Set<TextAnnotation> expectedNestedAnnotations = new HashSet<TextAnnotation>();
		expectedNestedAnnotations.add(getAnnot6());
		expectedNestedAnnotations.add(getAnnot7());

		assertEquals(expectedNestedAnnotations, nestedAnnotations);
	}

	@Test
	public void testIdentifyNestedAnnotations() throws FileNotFoundException, IOException {
		BufferedWriter logWriter = null;

		List<TextAnnotation> annotations = new ArrayList<TextAnnotation>();

		annotations.add(getAnnot1());
		annotations.add(getAnnot2());
		annotations.add(getAnnot3());
		annotations.add(getAnnot4());
		annotations.add(getAnnot5());
		annotations.add(getAnnot6());
		annotations.add(getAnnot7());
		annotations.add(getAnnot8());
		annotations.add(getAnnot9());
		annotations.add(getAnnot10());

		Set<TextAnnotation> nestedAnnotations = ExcludeCraftNestedConcepts.identifyNestedAnnotations(annotations,
				logWriter, ExcludeExactOverlaps.NO);

		Set<TextAnnotation> expectedNestedAnnotations = new HashSet<TextAnnotation>();
		expectedNestedAnnotations.add(getAnnot2());
		expectedNestedAnnotations.add(getAnnot6());
		expectedNestedAnnotations.add(getAnnot7());

		assertEquals(expectedNestedAnnotations, nestedAnnotations);
	}

	@Test
	public void testFilterNestedConceptAnnotations() throws FileNotFoundException, IOException {

		BufferedWriter logWriter = null;

		Map<Ont, TextDocument> ontToDocMap = getOntToDocMap();
		ExcludeCraftNestedConcepts.filterNestedConceptAnnotations(ontToDocMap, getDocText(), logWriter,
				ExcludeExactOverlaps.NO);

		TextDocument updatedUberonDoc = new TextDocument(DOC_ID, "unknown", getDocText());
		updatedUberonDoc.addAnnotation(getAnnot4());
		updatedUberonDoc.addAnnotation(getAnnot5());
		updatedUberonDoc.addAnnotation(getAnnot9());

		Map<Ont, TextDocument> expectedOntToDocMap = new HashMap<Ont, TextDocument>();
		expectedOntToDocMap.put(Ont.CL, getClDoc());
		expectedOntToDocMap.put(Ont.CHEBI, getChebiDoc());
		expectedOntToDocMap.put(Ont.UBERON, updatedUberonDoc);

		// check annotation counts first
		assertEquals(expectedOntToDocMap.get(Ont.CL).getAnnotations().size(),
				ontToDocMap.get(Ont.CL).getAnnotations().size());
		assertEquals(expectedOntToDocMap.get(Ont.CHEBI).getAnnotations().size(),
				ontToDocMap.get(Ont.CHEBI).getAnnotations().size());
		assertEquals(expectedOntToDocMap.get(Ont.UBERON).getAnnotations().size(),
				ontToDocMap.get(Ont.UBERON).getAnnotations().size());

		// then check the actual annotations
		assertEquals(expectedOntToDocMap.get(Ont.CL), ontToDocMap.get(Ont.CL));
		assertEquals(expectedOntToDocMap.get(Ont.CHEBI), ontToDocMap.get(Ont.CHEBI));
		assertEquals(expectedOntToDocMap.get(Ont.UBERON), ontToDocMap.get(Ont.UBERON));
	}
}
