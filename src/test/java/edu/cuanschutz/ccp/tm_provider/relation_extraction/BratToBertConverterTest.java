package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.BratToBertConverter.Assertion;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.BratToBertConverter.Recurse;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileComparisonUtil;
import edu.ucdenver.ccp.common.file.FileComparisonUtil.ColumnOrder;
import edu.ucdenver.ccp.common.file.FileComparisonUtil.LineOrder;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.ComplexSlotMention;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultComplexSlotMention;

public class BratToBertConverterTest {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private File bratFolder;
	private File annFile;
	private File txtFile;

	@Before
	public void setup() throws IOException {
		bratFolder = folder.newFolder();

		annFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "bl_chemical_to_disease.ann", bratFolder);
		txtFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "bl_chemical_to_disease.txt", bratFolder);
	}

	@Test
	public void testGetEntityAnnots() {

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation annot1 = factory.createAnnotation(0, 5, "null", "CHEBI:12345");
		TextAnnotation annot2 = factory.createAnnotation(0, 5, "null", "DRUGBANK:12345");

		TextAnnotation annot3 = factory.createAnnotation(0, 5, "null", "MONDO:12345");
		TextAnnotation annot4 = factory.createAnnotation(0, 5, "null", "HP:12345");

		Set<TextAnnotation> entityAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4));
		Map<BiolinkClass, Set<TextAnnotation>> map = BratToBertConverter.getEntityAnnots(biolinkAssociation,
				entityAnnots);

		Map<BiolinkClass, Set<TextAnnotation>> expectedMap = new HashMap<BiolinkClass, Set<TextAnnotation>>();
		CollectionsUtil.addToOne2ManyUniqueMap(BiolinkClass.CHEMICAL, annot1, expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(BiolinkClass.CHEMICAL, annot2, expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE, annot3, expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap(BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE, annot4, expectedMap);

		assertEquals(expectedMap, map);

	}

	@Test
	public void testGetTrainingExampleLine() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation sentenceAnnot = factory.createAnnotation(299, 392,
				"Lung fibrosis is frequently observed in cancer patients undergoing bleomycin (BLM) treatment.",
				"sentence");
		TextAnnotation diseaseAnnot = factory.createAnnotation(299, 312, "Lung fibrosis", "MONDO:12345");
		TextAnnotation chemicalAnnot = factory.createAnnotation(366, 375, "bleomycin", "DRUGBANK:12345");

		diseaseAnnot.setAnnotationID("entity1");
		chemicalAnnot.setAnnotationID("entity2");

		Assertion assertion = new Assertion(BiolinkClass.CHEMICAL, chemicalAnnot,
				BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE, diseaseAnnot, "treats");

		String trainingLine = BratToBertConverter.getTrainingExampleLine(sentenceAnnot, assertion,
				new HashSet<String>(), false);

		String sentenceWithPlaceholders = String.format(
				"%s is frequently observed in cancer patients undergoing %s (BLM) treatment.",
				BiolinkConstants.DISEASE_PLACEHOLDER, BiolinkConstants.CHEMICAL_PLACEHOLDER);
		String hash = DigestUtils.shaHex(sentenceWithPlaceholders);
		String expectedTrainingLine = String.format("%s\t%s\t%s", hash, sentenceWithPlaceholders, "treats");

		assertEquals(expectedTrainingLine, trainingLine);

	}

	@Test
	public void testCreateAllAssertions() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation diseaseAnnot1 = factory.createAnnotation(299, 312, "Lung fibrosis", "MONDO:12345");
		TextAnnotation diseaseAnnot2 = factory.createAnnotation(0, 12, "hypertension", "MONDO:7890");

		TextAnnotation chemicalAnnot1 = factory.createAnnotation(366, 375, "bleomycin", "DRUGBANK:12345");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(400, 405, "water", "CHEBI:7890");

		ComplexSlotMention csm = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm);

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;
		Set<TextAnnotation> subjectAnnots = new HashSet<TextAnnotation>(Arrays.asList(chemicalAnnot1, chemicalAnnot2));
		Set<TextAnnotation> objectAnnots = new HashSet<TextAnnotation>(Arrays.asList(diseaseAnnot1, diseaseAnnot2));

		Set<? extends Assertion> assertions = BratToBertConverter.createAllAssertions(biolinkAssociation, subjectAnnots,
				objectAnnots);

		// there's only one assertion that has a relationship between entities
		Assertion assertion1 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		// the other assertions represent "false" examples
		Assertion assertion2 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion3 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion4 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());

		Set<Assertion> expectedAssertions = new HashSet<Assertion>(
				Arrays.asList(assertion1, assertion2, assertion3, assertion4));

		assertEquals(expectedAssertions.size(), assertions.size());
		assertEquals(expectedAssertions, assertions);

	}

	@Test
	public void testCreateAllAssertions2() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation diseaseAnnot1 = factory.createAnnotation(299, 312, "Lung fibrosis", "MONDO:12345");
		TextAnnotation diseaseAnnot2 = factory.createAnnotation(339, 345, "cancer", "MONDO:7890");

		TextAnnotation chemicalAnnot1 = factory.createAnnotation(366, 375, "bleomycin", "DRUGBANK:12345");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(377, 380, "BLM", "CHEBI:7890");

		ComplexSlotMention csm1 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm1.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm1);

		ComplexSlotMention csm2 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm2.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm2);

		ComplexSlotMention csm3 = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm3.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm3);

		ComplexSlotMention csm4 = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm4.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm4);

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;
		Set<TextAnnotation> subjectAnnots = new HashSet<TextAnnotation>(Arrays.asList(chemicalAnnot1, chemicalAnnot2));
		Set<TextAnnotation> objectAnnots = new HashSet<TextAnnotation>(Arrays.asList(diseaseAnnot1, diseaseAnnot2));

		Set<? extends Assertion> assertions = BratToBertConverter.createAllAssertions(biolinkAssociation, subjectAnnots,
				objectAnnots);

		Assertion assertion1 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		Assertion assertion2 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		Assertion assertion3 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		Assertion assertion4 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());

		Set<Assertion> expectedAssertions = new HashSet<Assertion>(
				Arrays.asList(assertion1, assertion2, assertion3, assertion4));

		assertEquals(expectedAssertions.size(), assertions.size());
		assertEquals(expectedAssertions, assertions);

	}

	@Test
	public void testGetAssertionsInterclassRelationship() {
		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;
		Map<BiolinkClass, Set<TextAnnotation>> biolinkClassToEntityAnnotsMap = new HashMap<BiolinkClass, Set<TextAnnotation>>();

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation diseaseAnnot1 = factory.createAnnotation(299, 312, "Lung fibrosis", "MONDO:12345");
		TextAnnotation diseaseAnnot2 = factory.createAnnotation(0, 12, "hypertension", "MONDO:7890");

		TextAnnotation chemicalAnnot1 = factory.createAnnotation(366, 375, "bleomycin", "DRUGBANK:12345");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(400, 405, "water", "CHEBI:7890");

		ComplexSlotMention csm = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm);

		Set<TextAnnotation> subjectAnnots = new HashSet<TextAnnotation>(Arrays.asList(chemicalAnnot1, chemicalAnnot2));
		Set<TextAnnotation> objectAnnots = new HashSet<TextAnnotation>(Arrays.asList(diseaseAnnot1, diseaseAnnot2));

		biolinkClassToEntityAnnotsMap.put(biolinkAssociation.getSubjectClass(), subjectAnnots);
		biolinkClassToEntityAnnotsMap.put(biolinkAssociation.getObjectClass(), objectAnnots);

		Set<Assertion> assertions = BratToBertConverter.getAssertions(biolinkAssociation,
				biolinkClassToEntityAnnotsMap);

		// there's only one assertion that has a relationship between entities
		Assertion assertion1 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		// the other assertions represent "false" examples
		Assertion assertion2 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot1,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion3 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot2,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion4 = new Assertion(biolinkAssociation.getSubjectClass(), chemicalAnnot1,
				biolinkAssociation.getObjectClass(), diseaseAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());

		Set<Assertion> expectedAssertions = new HashSet<Assertion>(
				Arrays.asList(assertion1, assertion2, assertion3, assertion4));

		assertEquals(expectedAssertions.size(), assertions.size());
		assertEquals(expectedAssertions, assertions);
	}

	@Test
	public void testGetAssertionsIntraclassRelationship() {
		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_GENE_REGULATORY_RELATIONSHIP;
		Map<BiolinkClass, Set<TextAnnotation>> biolinkClassToEntityAnnotsMap = new HashMap<BiolinkClass, Set<TextAnnotation>>();

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		// here, gene1 positively regulates gene2 -- there are no other relationships
		// between the other genes

		TextAnnotation geneAnnot1 = factory.createAnnotation(299, 312, "gene1", "PR:12345");
		TextAnnotation geneAnnot2 = factory.createAnnotation(0, 12, "gene2", "PR:7890");

		TextAnnotation geneAnnot3 = factory.createAnnotation(366, 375, "gene3", "PR:612345");
		TextAnnotation geneAnnot4 = factory.createAnnotation(400, 405, "gene4", "PR:67890");

		ComplexSlotMention csm = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY.getEdgeLabelAbbreviation());
		csm.addClassMention(geneAnnot2.getClassMention());
		geneAnnot1.getClassMention().addComplexSlotMention(csm);

		Set<TextAnnotation> geneAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(geneAnnot1, geneAnnot2, geneAnnot3, geneAnnot4));

		biolinkClassToEntityAnnotsMap.put(biolinkAssociation.getSubjectClass(), geneAnnots);

		Set<Assertion> assertions = BratToBertConverter.getAssertions(biolinkAssociation,
				biolinkClassToEntityAnnotsMap);

		// there's only one assertion that has a relationship between entities
		Assertion assertion1 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot1,
				biolinkAssociation.getObjectClass(), geneAnnot2,
				BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY.getEdgeLabelAbbreviation());
		// the other assertions represent "false" examples
		Assertion assertion3 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot1,
				biolinkAssociation.getObjectClass(), geneAnnot3,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion4 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot1,
				biolinkAssociation.getObjectClass(), geneAnnot4,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion2 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot2,
				biolinkAssociation.getObjectClass(), geneAnnot1,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion5 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot2,
				biolinkAssociation.getObjectClass(), geneAnnot3,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion6 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot2,
				biolinkAssociation.getObjectClass(), geneAnnot4,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion7 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot3,
				biolinkAssociation.getObjectClass(), geneAnnot1,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion8 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot3,
				biolinkAssociation.getObjectClass(), geneAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion9 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot3,
				biolinkAssociation.getObjectClass(), geneAnnot4,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion10 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot4,
				biolinkAssociation.getObjectClass(), geneAnnot1,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion11 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot4,
				biolinkAssociation.getObjectClass(), geneAnnot2,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
		Assertion assertion12 = new Assertion(biolinkAssociation.getSubjectClass(), geneAnnot4,
				biolinkAssociation.getObjectClass(), geneAnnot3,
				BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());

		Set<Assertion> expectedAssertions = new HashSet<Assertion>(
				Arrays.asList(assertion1, assertion2, assertion3, assertion4, assertion5, assertion6, assertion7,
						assertion8, assertion9, assertion10, assertion11, assertion12));

		assertEquals(expectedAssertions.size(), assertions.size());
		assertEquals(expectedAssertions, assertions);
	}

	@Test
	public void testGetCorrespondingTxtFile() {
		File correspondingTxtFile = BratToBertConverter.getCorrespondingTxtFile(annFile);
		assertEquals(txtFile.getAbsolutePath(), correspondingTxtFile.getAbsolutePath());
	}

	@Test
	public void testGenerateBertStyleTrainingDataWithIds() throws FileNotFoundException, IOException {

		File outputFile = folder.newFile();

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation sentenceAnnot2 = factory.createAnnotation(299, 392,
				"Lung fibrosis is frequently observed in cancer patients undergoing bleomycin (BLM) treatment.",
				"sentence");
		TextAnnotation diseaseAnnot1 = factory.createAnnotation(299, 312, "Lung fibrosis", "MONDO:12345");
		TextAnnotation chemicalAnnot1 = factory.createAnnotation(366, 375, "bleomycin", "DRUGBANK:12345");
		ComplexSlotMention csm = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm);

		TextAnnotation diseaseAnnot2 = factory.createAnnotation(339, 345, "cancer", "MONDO:612345");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(377, 380, "BLM", "DRUGBANK:12345");

		ComplexSlotMention csm2 = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm2.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm2);

		ComplexSlotMention csm3 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm3.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm3);

		ComplexSlotMention csm4 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm4.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm4);

		// bleomycin contributes_to lung fibrosis
		// BLM contributes_to lung fibrosis
		// bleomycin treats cancer
		// BLM treats cancer

		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, 298,
				"We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (IPMN) dysplasia than individual biomarkers alone.",
				"sentence");
		TextAnnotation chemicalAnnot3 = factory.createAnnotation(50, 66, "prostaglandin E2", "CHEBI:112345");
		TextAnnotation chemicalAnnot4 = factory.createAnnotation(139, 151, "carbohydrate", "CHEBI:212345");
		TextAnnotation chemicalAnnot5 = factory.createAnnotation(152, 159, "antigen", "CHEBI:312345");
		TextAnnotation chemicalAnnot6 = factory.createAnnotation(264, 274, "biomarkers", "CHEBI:412345");
		TextAnnotation diseaseAnnot3 = factory.createAnnotation(213, 230, "mucinous neoplasm", "MONDO:112345");
		TextAnnotation diseaseAnnot4 = factory.createAnnotation(232, 236, "IPMN", "MONDO:212345");

		List<TextAnnotation> sentences = Arrays.asList(sentenceAnnot1, sentenceAnnot2);
		List<TextAnnotation> entityAnnots = Arrays.asList(chemicalAnnot3, chemicalAnnot4, chemicalAnnot4,
				chemicalAnnot5, diseaseAnnot3, diseaseAnnot4, chemicalAnnot6, diseaseAnnot1, diseaseAnnot2,
				chemicalAnnot1, chemicalAnnot2);

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			BratToBertConverter.generateBertStyleTrainingData(biolinkAssociation, sentences, entityAnnots, writer, new HashSet<String>());
		}

		// @formatter:off
		String sentenceWithPlaceholders1 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders2 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders3 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders4 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders5 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders6 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders7 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual %s alone.",BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder() ,BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders8 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual %s alone.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders9 = String.format("%s is frequently observed in cancer patients undergoing %s (BLM) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders10 = String.format("%s is frequently observed in cancer patients undergoing bleomycin (%s) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders11 = String.format("Lung fibrosis is frequently observed in %s patients undergoing %s (BLM) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders12 = String.format("Lung fibrosis is frequently observed in %s patients undergoing bleomycin (%s) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		
		List<String> expectedLines = Arrays.asList(
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders1), sentenceWithPlaceholders1, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders2), sentenceWithPlaceholders2, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders3), sentenceWithPlaceholders3, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders4), sentenceWithPlaceholders4, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders5), sentenceWithPlaceholders5, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders6), sentenceWithPlaceholders6, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders7), sentenceWithPlaceholders7, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders8), sentenceWithPlaceholders8, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders9), sentenceWithPlaceholders9, BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders10), sentenceWithPlaceholders10, BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders11), sentenceWithPlaceholders11, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders12), sentenceWithPlaceholders12, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()));
		// @formatter:on

		assertTrue(FileComparisonUtil.hasExpectedLines(outputFile, UTF8, expectedLines, null, LineOrder.ANY_ORDER,
				ColumnOrder.AS_IN_FILE));
	}

	@Test
	public void testGenerateBertStyleTrainingDataWithTypes() throws FileNotFoundException, IOException {

		File outputFile = folder.newFile();

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		TextAnnotation sentenceAnnot2 = factory.createAnnotation(299, 392,
				"Lung fibrosis is frequently observed in cancer patients undergoing bleomycin (BLM) treatment.",
				"sentence");
		TextAnnotation diseaseAnnot1 = factory.createAnnotation(299, 312, "Lung fibrosis", "disease");
		TextAnnotation chemicalAnnot1 = factory.createAnnotation(366, 375, "bleomycin", "chemical");
		ComplexSlotMention csm = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm);

		TextAnnotation diseaseAnnot2 = factory.createAnnotation(339, 345, "cancer", "disease");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(377, 380, "BLM", "chemical");

		ComplexSlotMention csm2 = new DefaultComplexSlotMention(
				BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation());
		csm2.addClassMention(diseaseAnnot1.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm2);

		ComplexSlotMention csm3 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm3.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot1.getClassMention().addComplexSlotMention(csm3);

		ComplexSlotMention csm4 = new DefaultComplexSlotMention(BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation());
		csm4.addClassMention(diseaseAnnot2.getClassMention());
		chemicalAnnot2.getClassMention().addComplexSlotMention(csm4);

		// bleomycin contributes_to lung fibrosis
		// BLM contributes_to lung fibrosis
		// bleomycin treats cancer
		// BLM treats cancer

		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, 298,
				"We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (IPMN) dysplasia than individual biomarkers alone.",
				"sentence");
		TextAnnotation chemicalAnnot3 = factory.createAnnotation(50, 66, "prostaglandin E2", "chemical");
		TextAnnotation chemicalAnnot4 = factory.createAnnotation(139, 151, "carbohydrate", "chemical");
		TextAnnotation chemicalAnnot5 = factory.createAnnotation(152, 159, "antigen", "chemical");
		TextAnnotation chemicalAnnot6 = factory.createAnnotation(264, 274, "biomarkers", "chemical");
		TextAnnotation diseaseAnnot3 = factory.createAnnotation(213, 230, "mucinous neoplasm", "disease");
		TextAnnotation diseaseAnnot4 = factory.createAnnotation(232, 236, "IPMN", "disease");

		List<TextAnnotation> sentences = Arrays.asList(sentenceAnnot1, sentenceAnnot2);
		List<TextAnnotation> entityAnnots = Arrays.asList(chemicalAnnot3, chemicalAnnot4, chemicalAnnot4,
				chemicalAnnot5, diseaseAnnot3, diseaseAnnot4, chemicalAnnot6, diseaseAnnot1, diseaseAnnot2,
				chemicalAnnot1, chemicalAnnot2);

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			BratToBertConverter.generateBertStyleTrainingData(biolinkAssociation, sentences, entityAnnots, writer, new HashSet<String>());
		}

		// @formatter:off
		String sentenceWithPlaceholders1 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders2 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders3 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders4 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders5 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders6 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone.", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
		String sentenceWithPlaceholders7 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual %s alone.",BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder() ,BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders8 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual %s alone.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders9 = String.format("%s is frequently observed in cancer patients undergoing %s (BLM) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders10 = String.format("%s is frequently observed in cancer patients undergoing bleomycin (%s) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders11 = String.format("Lung fibrosis is frequently observed in %s patients undergoing %s (BLM) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		String sentenceWithPlaceholders12 = String.format("Lung fibrosis is frequently observed in %s patients undergoing bleomycin (%s) treatment.", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
		
		List<String> expectedLines = Arrays.asList(
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders1), sentenceWithPlaceholders1, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders2), sentenceWithPlaceholders2, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders3), sentenceWithPlaceholders3, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders4), sentenceWithPlaceholders4, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders5), sentenceWithPlaceholders5, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders6), sentenceWithPlaceholders6, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders7), sentenceWithPlaceholders7, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders8), sentenceWithPlaceholders8, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders9), sentenceWithPlaceholders9, BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders10), sentenceWithPlaceholders10, BiolinkPredicate.BL_CONTRIBUTES_TO.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders11), sentenceWithPlaceholders11, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()),
				String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders12), sentenceWithPlaceholders12, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()));
		// @formatter:on

		assertTrue(FileComparisonUtil.hasExpectedLines(outputFile, UTF8, expectedLines, null, LineOrder.ANY_ORDER,
				ColumnOrder.AS_IN_FILE));
	}

	@Test
	public void testConvertBratToBert() throws FileNotFoundException, IOException {
		File outputFile = folder.newFile();

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;
		BratToBertConverter.convertBratToBert(biolinkAssociation, Arrays.asList(bratFolder), Recurse.NO, outputFile);

		// @formatter:off
				String sentenceWithPlaceholders1 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders2 = String.format("We sought to determine if interleukin (IL)-1β and %s (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders3 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders4 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum %s antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders5 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders6 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate %s (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual biomarkers alone. -- PMID:31404023", BiolinkClass.CHEMICAL.getPlaceholder(), BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder());
				String sentenceWithPlaceholders7 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary %s (IPMN) dysplasia than individual %s alone. -- PMID:31404023",BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder() ,BiolinkClass.CHEMICAL.getPlaceholder());
				String sentenceWithPlaceholders8 = String.format("We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (%s) dysplasia than individual %s alone. -- PMID:31404023", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
				String sentenceWithPlaceholders9 = String.format("%s is frequently observed in cancer patients undergoing %s (BLM) treatment. -- PMID:31256436", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
				String sentenceWithPlaceholders10 = String.format("%s is frequently observed in cancer patients undergoing bleomycin (%s) treatment. -- PMID:31256436", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
				String sentenceWithPlaceholders11 = String.format("Lung fibrosis is frequently observed in %s patients undergoing %s (BLM) treatment. -- PMID:31256436", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
				String sentenceWithPlaceholders12 = String.format("Lung fibrosis is frequently observed in %s patients undergoing bleomycin (%s) treatment. -- PMID:31256436", BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.getPlaceholder(),BiolinkClass.CHEMICAL.getPlaceholder());
				
				List<String> expectedLines = Arrays.asList(
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders1), sentenceWithPlaceholders1, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders2), sentenceWithPlaceholders2, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders3), sentenceWithPlaceholders3, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders4), sentenceWithPlaceholders4, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders5), sentenceWithPlaceholders5, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders6), sentenceWithPlaceholders6, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders7), sentenceWithPlaceholders7, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders8), sentenceWithPlaceholders8, BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders9), sentenceWithPlaceholders9, "causes"),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders10), sentenceWithPlaceholders10, "causes"),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders11), sentenceWithPlaceholders11, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()),
						String.format("%s\t%s\t%s", DigestUtils.shaHex(sentenceWithPlaceholders12), sentenceWithPlaceholders12, BiolinkPredicate.BL_TREATS.getEdgeLabelAbbreviation()));
				// @formatter:on

		assertTrue(FileComparisonUtil.hasExpectedLines(outputFile, UTF8, expectedLines, null, LineOrder.ANY_ORDER,
				ColumnOrder.AS_IN_FILE));

	}

	@Test
	public void testGetSentenceAnnotationsOnePerLine() throws IOException {
		List<TextAnnotation> sentenceAnnots = BratToBertConverter.getSentenceAnnotationsOnePerLine(txtFile);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextAnnotation sentenceAnnot1 = factory.createAnnotation(0, 298,
				"We sought to determine if interleukin (IL)-1β and prostaglandin E2 (PGE2) (inflammatory mediators in pancreatic fluid) together with serum carbohydrate antigen (CA) 19-9 could better predict intraductal papillary mucinous neoplasm (IPMN) dysplasia than individual biomarkers alone. -- PMID:31404023",
				"sentence");
		TextAnnotation sentenceAnnot2 = factory.createAnnotation(299, 409,
				"Lung fibrosis is frequently observed in cancer patients undergoing bleomycin (BLM) treatment. -- PMID:31256436",
				"sentence");

		// settings below required to get sentence annotations to match those produced
		// by getSentenceAnnotationsOnePerLine
		sentenceAnnot1.setAnnotationSets(Collections.emptySet());
		sentenceAnnot1.setAnnotator(new Annotator(null, "OpenNLP", "OpenNLP"));
		sentenceAnnot1.setDocumentID("-1");
		sentenceAnnot2.setAnnotationSets(Collections.emptySet());
		sentenceAnnot2.setAnnotator(new Annotator(null, "OpenNLP", "OpenNLP"));
		sentenceAnnot2.setDocumentID("-1");

		List<TextAnnotation> expectedSentenceAnnots = Arrays.asList(sentenceAnnot1, sentenceAnnot2);

		assertEquals(expectedSentenceAnnots, sentenceAnnots);
	}

	@Test
	public void testNormalizeEntityTypes() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextAnnotation chemicalAnnot1 = factory.createAnnotation(50, 66, "prostaglandin E2", "corrected_chemical");
		TextAnnotation chemicalAnnot1a = factory.createAnnotation(50, 66, "prostaglandin E2", "chemical");
		TextAnnotation chemicalAnnot2 = factory.createAnnotation(64, 66, "E2", "chemical");

		TextAnnotation chemicalAnnot3 = factory.createAnnotation(139, 151, "carbohydrate", "chemical");
		TextAnnotation chemicalAnnot4 = factory.createAnnotation(152, 159, "antigen", "missed_chemical");
		TextAnnotation chemicalAnnot4a = factory.createAnnotation(152, 159, "antigen", "chemical");
		TextAnnotation diseaseAnnot1 = factory.createAnnotation(213, 230, "mucinous neoplasm", "corrected_disease");
		TextAnnotation diseaseAnnot1a = factory.createAnnotation(213, 230, "mucinous neoplasm",
				"disease_or_phenotypic_feature");
		TextAnnotation diseaseAnnot2 = factory.createAnnotation(222, 230, "neoplasm", "disease");
		TextAnnotation diseaseAnnot3 = factory.createAnnotation(232, 236, "IPMN", "missed_disease");
		TextAnnotation diseaseAnnot3a = factory.createAnnotation(232, 236, "IPMN", "disease_or_phenotypic_feature");
		TextAnnotation chemicalAnnot5 = factory.createAnnotation(264, 274, "biomarkers", "chemical");

		List<TextAnnotation> normalizedEntityAnnots = BratToBertConverter.normalizeEntityTypes(
				BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE,
				Arrays.asList(chemicalAnnot1, chemicalAnnot2, chemicalAnnot3, chemicalAnnot4, diseaseAnnot1,
						diseaseAnnot2, diseaseAnnot3, chemicalAnnot5));

		List<TextAnnotation> expectedNormalizedEntityAnnots = Arrays.asList(chemicalAnnot1a, chemicalAnnot3,
				chemicalAnnot4a, diseaseAnnot1a, diseaseAnnot3a, chemicalAnnot5);

		Collections.sort(normalizedEntityAnnots, TextAnnotation.BY_SPAN());
		Collections.sort(expectedNormalizedEntityAnnots, TextAnnotation.BY_SPAN());
		assertEquals(expectedNormalizedEntityAnnots.size(), normalizedEntityAnnots.size());
		assertEquals(expectedNormalizedEntityAnnots, normalizedEntityAnnots);
	}

}
