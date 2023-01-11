package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileComparisonUtil;
import edu.ucdenver.ccp.common.file.FileComparisonUtil.ColumnOrder;
import edu.ucdenver.ccp.common.file.FileComparisonUtil.LineOrder;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ElasticsearchToBratExporterTest {
	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testBuildSentenceQuery() throws IOException {
		Set<Set<String>> ontologyPrefixes = new HashSet<Set<String>>();
		ontologyPrefixes.add(CollectionsUtil.createSet("DRUGBANK", "CHEBI"));
		ontologyPrefixes.add(CollectionsUtil.createSet("UBERON", "CL"));
		ontologyPrefixes.add(CollectionsUtil.createSet("GO"));
		String sentenceQuery = ElasticsearchToBratExporter.buildSentenceQuery(ontologyPrefixes);

		// @formatter:off
		String expectedSentenceQuery = "{\n" + 
				"		\"bool\": {\n" + 
				"			\"must\": [\n" + 
				"				{\n" + 
				"					\"match\": {\n" + 
				"						\"annotatedText\": {\n" + 
				"							\"query\": \"_CHEBI _DRUGBANK\",\n" + 
				"							\"operator\": \"or\"\n" + 
				"						}\n" + 
				"					}\n" + 
				"				},\n" + 
				"				{\n" + 
				"					\"match\": {\n" + 
				"						\"annotatedText\": {\n" + 
				"							\"query\": \"_CL _UBERON\",\n" + 
				"							\"operator\": \"or\"\n" + 
				"						}\n" + 
				"					}\n" + 
				"				},\n" + 
				"				{\n" + 
				"					\"match\": {\n" + 
				"						\"annotatedText\": {\n" + 
				"							\"query\": \"_GO\"\n" + 
				"						}\n" + 
				"					}\n" + 
				"				}\n" + 
				"			]\n" + 
				"		}\n" + 
				"}";
		// @formatter:on

		// the leading whitespace is different (indented in expectedSentenceQuery, but
		// not in the autogenerated version so we remove whitespace before making the
		// comparison.
		assertEquals(expectedSentenceQuery.replaceAll("\\s", ""), sentenceQuery.replaceAll("\\s", ""));
	}

	@Test
	public void testBuildAnnotatedTextMatchStanza() throws IOException {
		String annotatedTextMatchTemplate = ClassPathUtil.getContentsFromClasspathResource(
				ElasticsearchToBratExporter.class, ElasticsearchToBratExporter.ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE,
				UTF8);
		String ontologyPrefixQueryString = "CHEBI DRUGBANK";
		String matchStanza = ElasticsearchToBratExporter.createAnnotatedTextMatchStanza(annotatedTextMatchTemplate,
				ontologyPrefixQueryString);

		// @formatter:off
		String expectedMatchStanza = "{\n" + 
				"	\"match\": {\n" + 
				"		\"annotatedText\": {\n" + 
				"			\"query\": \"CHEBI DRUGBANK\",\n" + 
				"			\"operator\": \"or\"\n" + 
				"		}\n" + 
				"	}\n" + 
				"}";
		// @formatter:on

		assertEquals(expectedMatchStanza, matchStanza);

	}

	@Test
	public void testExcludeBasedOnEntityIdsOnlySingleEntity() {

		Set<BiolinkClass> biolinkClasses = CollectionsUtil.createSet(BiolinkClass.CHEMICAL, BiolinkClass.DISEASE);
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextDocument td = new TextDocument("PMID:12345", "PubMed", "Here is some text.");
		TextAnnotation annot1 = factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945");
		td.addAnnotation(annot1);

		TextDocument updatedTd = ElasticsearchToBratExporter.excludeBasedOnEntityIds(td,
				ElasticsearchToBratExporter.IDENTIFIERS_TO_EXCLUDE, biolinkClasses);

		assertNull(updatedTd);

	}

	@Test
	public void testExcludeBasedOnEntityIds() {

		Set<BiolinkClass> biolinkClasses = CollectionsUtil.createSet(BiolinkClass.CHEMICAL, BiolinkClass.DISEASE);
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextDocument td = new TextDocument("PMID:12345", "PubMed", "Here is some text.");
		TextAnnotation annot1 = factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945");
		TextAnnotation annot2 = factory.createAnnotation(0, 10, "Hemoglobin", "MONDO:12345");
		td.addAnnotation(annot1);
		td.addAnnotation(annot2);

		TextDocument updatedTd = ElasticsearchToBratExporter.excludeBasedOnEntityIds(td,
				ElasticsearchToBratExporter.IDENTIFIERS_TO_EXCLUDE, biolinkClasses);

		Set<TextAnnotation> expectedAnnots = CollectionsUtil.createSet(annot1, annot2);

		assertNotNull(updatedTd);
		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(updatedTd.getAnnotations()));

	}

	@Test
	public void testExcludeBasedOnEntityIdsFilterSomeOut() {

		Set<BiolinkClass> biolinkClasses = CollectionsUtil.createSet(BiolinkClass.CHEMICAL, BiolinkClass.DISEASE);
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextDocument td = new TextDocument("PMID:12345", "PubMed", "Here is some text.");
		TextAnnotation annot1 = factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945");
		TextAnnotation annot2 = factory.createAnnotation(0, 10, "Hemoglobin", "MONDO:12345");
		TextAnnotation annot3 = factory.createAnnotation(0, 10, "Hemoglobin", "GO:12345");
		TextAnnotation annot4 = factory.createAnnotation(0, 10, "Hemoglobin", "CHEBI:12345");
		td.addAnnotation(annot1);
		td.addAnnotation(annot2);
		td.addAnnotation(annot3);
		td.addAnnotation(annot4);

		TextDocument updatedTd = ElasticsearchToBratExporter.excludeBasedOnEntityIds(td,
				ElasticsearchToBratExporter.IDENTIFIERS_TO_EXCLUDE, biolinkClasses);

		Set<TextAnnotation> expectedAnnots = CollectionsUtil.createSet(annot1, annot2, annot4);

		assertNotNull(updatedTd);
		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(updatedTd.getAnnotations()));

	}

	@Test
	public void testDeserializeAnnotatedText() {

		Set<String> ontologyPrefixes = CollectionsUtil.createSet("DRUGBANK");
		String annotatedText = "(Hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q disorders including (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Iran, (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Thailand, and (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-India are important (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] variants.";
		TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
				ontologyPrefixes, null);
		// 1 2 3 4 5 6 7 8 9 0 1 2 3
		// 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
		String expectedText = "Hemoglobin Q disorders including hemoglobin Q-Iran, hemoglobin Q-Thailand, and hemoglobin Q-India are important hemoglobin variants.";
		assertEquals(expectedText, td.getText());

		Set<TextAnnotation> expectedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "GO:0005833"));

		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(td.getAnnotations()));

	}

	@Test
	public void testDeserializeAnnotatedTextWithEncodedText() {

		Set<String> ontologyPrefixes = CollectionsUtil.createSet("DRUGBANK");
		String annotatedText = "(Hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q disorders including %2528(hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Iran, (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Thailand, and (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-India%2529 are important (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] variants.";
		TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
				ontologyPrefixes, null);
		// 1 2 3 4 5 6 7 8 9 0 1 2 3
		// 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
		String expectedText = "Hemoglobin Q disorders including (hemoglobin Q-Iran, hemoglobin Q-Thailand, and hemoglobin Q-India) are important hemoglobin variants.";
		assertEquals(expectedText, td.getText());

		Set<TextAnnotation> expectedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945"));

		expectedAnnots.add(factory.createAnnotation(34, 44, "hemoglobin", "DRUGBANK:DB04945"));

		expectedAnnots.add(factory.createAnnotation(53, 63, "hemoglobin", "DRUGBANK:DB04945"));

		expectedAnnots.add(factory.createAnnotation(80, 90, "hemoglobin", "DRUGBANK:DB04945"));

		expectedAnnots.add(factory.createAnnotation(114, 124, "hemoglobin", "DRUGBANK:DB04945"));

		assertEquals(expectedAnnots.size(), new HashSet<TextAnnotation>(td.getAnnotations()).size());
		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(td.getAnnotations()));

	}

	@Test
	public void testGetRandomIndexes() {
		int maxSentenceCount = 4;
		int batchSize = 4;
		int redundantSentencesToIncludeCount = 0;
		for (int i = 0; i < 100; i++) {
			List<Integer> randomIndexes = ElasticsearchToBratExporter.getRandomIndexes(maxSentenceCount, batchSize,
					redundantSentencesToIncludeCount);
			assertTrue(randomIndexes.contains(0));
			assertTrue(randomIndexes.contains(1));
			assertTrue(randomIndexes.contains(2));
			assertTrue(randomIndexes.contains(3));
		}
	}

	@Test
	public void testGetRandomIndexesWithRedundantIndexes() {
		int maxSentenceCount = 4;
		int batchSize = 6;
		int redundantSentencesToIncludeCount = 2;
		for (int i = 0; i < 100; i++) {
			List<Integer> randomIndexes = ElasticsearchToBratExporter.getRandomIndexes(maxSentenceCount, batchSize,
					redundantSentencesToIncludeCount);
			assertTrue(randomIndexes.contains(0));
			assertTrue(randomIndexes.contains(1));
			assertTrue(randomIndexes.contains(2));
			assertTrue(randomIndexes.contains(3));
			assertTrue(randomIndexes.contains(-1));

			// there should be two -1's
			int minusOneCount = 0;
			for (Integer index : randomIndexes) {
				if (index == -1) {
					minusOneCount++;
				}
			}
			assertEquals(2, minusOneCount);
		}
	}

	@Test
	public void testGetRandomIndexesWithRedundantIndexes2() {
		int maxSentenceCount = 4;
		int batchSize = 4;
		int redundantSentencesToIncludeCount = 2;
		for (int i = 0; i < 100; i++) {
			List<Integer> randomIndexes = ElasticsearchToBratExporter.getRandomIndexes(maxSentenceCount, batchSize,
					redundantSentencesToIncludeCount);
			assertTrue(randomIndexes.contains(-1));

			// there should be two -1's
			int minusOneCount = 0;
			for (Integer index : randomIndexes) {
				if (index == -1) {
					minusOneCount++;
				}
			}
			assertEquals(2, minusOneCount);
		}

	}

//	T index not incrementing properly
//	duplicate annotations end up in .ann files

	@Test
	public void testCreateBratFiles() throws IOException {
		File outputDirectory = folder.newFolder();
		File previousSentenceIdsFile = folder.newFile();

		List<String> annotatedTexts = Arrays.asList(
				"The mean overall qualitative (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]-to-background contrast grades for the T2-weighted (sequence)[SO_0000110\u0026_SO] were (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(muscle)[UBERON_0002385\u0026UBERON_0005090\u0026_UBERON] \u003d 2.84, (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(fat)[UBERON_0001013\u0026_UBERON] \u003d 2.20, and (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(mucosa)[UBERON_0000344\u0026_UBERON] \u003d 1.23, and for the contrast-enhanced T1-weighted (sequence)[SO_0000110\u0026_SO], they were (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(muscle)[UBERON_0002385\u0026UBERON_0005090\u0026_UBERON] \u003d 2.02, (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(fat)[UBERON_0001013\u0026_UBERON] \u003d 1.58, and (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(mucosa)[UBERON_0000344\u0026_UBERON] \u003d 0.73.",
				"There were 15 (fourth ventricle)[UBERON_0002422\u0026_UBERON] (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO], 6 (lateral ventricle)[UBERON_0002285\u0026_UBERON] (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO], and 5 spinal (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO].",
				"(Tuberous sclerosis complex)[MONDO_0019341\u0026_MONDO] %2528(TSC)[MONDO_0001734\u0026_MONDO]%2529 is a (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] suppressor (gene)[SO_0000704\u0026_SO] syndrome with manifestations that can include (seizures)[HP_0001250\u0026_HP], (mental retardation)[HP_0001249\u0026_HP], (autism)[HP_0000717\u0026MONDO_0005260\u0026_HP\u0026_MONDO], and (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] in the (brain)[UBERON_0000955\u0026_UBERON], (retina)[UBERON_0000966\u0026_UBERON], (kidney)[UBERON_0002113\u0026_UBERON], (heart)[UBERON_0000948\u0026_UBERON], and (skin)[UBERON_0000014\u0026UBERON_0002097\u0026_UBERON].",
				"CR were 95.6%25 %252844/46 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the (gallbladder)[UBERON_0002110\u0026_UBERON], 92.9%25%252879/85 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the (diaphragm)[UBERON_0001103\u0026_UBERON], 90.9%25%252840/44 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the gastrointestinal tract, 91.2%25 %252831/34 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near large (vessel)[UBERON_0000055\u0026_UBERON].");

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_DISEASE_OR_PHENOTYPIC_FEATURE_TO_LOCATION;
		String batchId = "1";

		List<TextDocument> inputSentences = new ArrayList<TextDocument>();
		Set<String> ontologyPrefixes = new HashSet<String>();
		ontologyPrefixes.addAll(biolinkAssociation.getSubjectClass().getOntologyPrefixes());
		ontologyPrefixes.addAll(biolinkAssociation.getObjectClass().getOntologyPrefixes());
		for (String annotatedText : annotatedTexts) {
			TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
					ontologyPrefixes, null);
			inputSentences.add(td);
		}

		assertEquals(4, inputSentences.size());
		int batchSize = 4;
		int sentencesPerPage = 4;
		Set<String> identifiersToExclude = new HashSet<String>();
		ElasticsearchToBratExporter.createBratFiles(outputDirectory, biolinkAssociation, batchId, batchSize,
				inputSentences, previousSentenceIdsFile, identifiersToExclude, sentencesPerPage,
				Collections.emptyList(), false, null, 0f);

		File annFile = new File(outputDirectory,
				String.format("%s_%s_0.ann", biolinkAssociation.name().toLowerCase(), batchId));
		File txtFile = new File(outputDirectory,
				String.format("%s_%s_0.txt", biolinkAssociation.name().toLowerCase(), batchId));

		assertTrue(annFile.exists());
		assertTrue(txtFile.exists());

		for (String line : FileReaderUtil.loadLinesFromFile(txtFile, UTF8)) {
			System.out.println(line);
		}

		// @formatter:off
		List<String> expectedAnnFileLines = Arrays.asList(
				"T1	disease_or_phenotypic_feature 29 34	tumor", 
				"T2	disease_or_phenotypic_feature 99 104	tumor", 
				"T3	anatomical_site 105 111	muscle", 
				"T4	disease_or_phenotypic_feature 120 125	tumor", 
				"T5	anatomical_site 126 129	fat", 
				"T6	disease_or_phenotypic_feature 142 147	tumor", 
				"T7	anatomical_site 148 154	mucosa", 
				"T8	disease_or_phenotypic_feature 225 230	tumor", 
				"T9	anatomical_site 231 237	muscle", 
				"T10	disease_or_phenotypic_feature 246 251	tumor", 
				"T11	anatomical_site 252 255	fat", 
				"T12	disease_or_phenotypic_feature 268 273	tumor", 
				"T13	anatomical_site 274 280	mucosa", 
				"T14	anatomical_site 303 319	fourth ventricle", 
				"T15	disease_or_phenotypic_feature 320 326	tumors", 
				"T16	anatomical_site 330 347	lateral ventricle", 
				"T17	disease_or_phenotypic_feature 348 354	tumors", 
				"T18	disease_or_phenotypic_feature 369 375	tumors", 
				"T19	disease_or_phenotypic_feature 377 403	Tuberous sclerosis complex", 
				"T20	disease_or_phenotypic_feature 405 408	TSC", 
				"T21	disease_or_phenotypic_feature 415 420	tumor", 
				"T22	disease_or_phenotypic_feature 483 491	seizures", 
				"T23	disease_or_phenotypic_feature 493 511	mental retardation", 
				"T24	disease_or_phenotypic_feature 513 519	autism", 
				"T25	disease_or_phenotypic_feature 525 531	tumors", 
				"T26	anatomical_site 539 544	brain", 
				"T27	anatomical_site 546 552	retina", 
				"T28	anatomical_site 554 560	kidney", 
				"T29	anatomical_site 562 567	heart", 
				"T30	anatomical_site 573 577	skin", 
				"T31	disease_or_phenotypic_feature 600 606	tumors", 
				"T32	disease_or_phenotypic_feature 612 618	tumors", 
				"T33	anatomical_site 628 639	gallbladder", 
				"T34	disease_or_phenotypic_feature 653 659	tumors", 
				"T35	disease_or_phenotypic_feature 665 671	tumors", 
				"T36	anatomical_site 681 690	diaphragm", 
				"T37	disease_or_phenotypic_feature 704 710	tumors", 
				"T38	disease_or_phenotypic_feature 716 722	tumors", 
				"T39	disease_or_phenotypic_feature 769 775	tumors", 
				"T40	disease_or_phenotypic_feature 781 787	tumors", 
				"T41	anatomical_site 799 805	vessel"
				);
		// @formatter:on

		assertTrue(FileComparisonUtil.hasExpectedLines(annFile, UTF8, expectedAnnFileLines, null, LineOrder.AS_IN_FILE,
				ColumnOrder.AS_IN_FILE));

	}

	@Test
	public void testCreateBratFilesWithAdditionalRedundantSentences() throws IOException {
		File outputDirectory = folder.newFolder();
		File previousSentenceIdsFile = folder.newFile();

		List<String> annotatedTexts = Arrays.asList(
				"The mean overall qualitative (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]-to-background contrast grades for the T2-weighted (sequence)[SO_0000110\u0026_SO] were (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(muscle)[UBERON_0002385\u0026UBERON_0005090\u0026_UBERON] \u003d 2.84, (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(fat)[UBERON_0001013\u0026_UBERON] \u003d 2.20, and (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(mucosa)[UBERON_0000344\u0026_UBERON] \u003d 1.23, and for the contrast-enhanced T1-weighted (sequence)[SO_0000110\u0026_SO], they were (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(muscle)[UBERON_0002385\u0026UBERON_0005090\u0026_UBERON] \u003d 2.02, (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(fat)[UBERON_0001013\u0026_UBERON] \u003d 1.58, and (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]/(mucosa)[UBERON_0000344\u0026_UBERON] \u003d 0.73.",
				"There were 15 (fourth ventricle)[UBERON_0002422\u0026_UBERON] (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO], 6 (lateral ventricle)[UBERON_0002285\u0026_UBERON] (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO], and 5 spinal (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO].",
				"(Tuberous sclerosis complex)[MONDO_0019341\u0026_MONDO] %2528(TSC)[MONDO_0001734\u0026_MONDO]%2529 is a (tumor)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] suppressor (gene)[SO_0000704\u0026_SO] syndrome with manifestations that can include (seizures)[HP_0001250\u0026_HP], (mental retardation)[HP_0001249\u0026_HP], (autism)[HP_0000717\u0026MONDO_0005260\u0026_HP\u0026_MONDO], and (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] in the (brain)[UBERON_0000955\u0026_UBERON], (retina)[UBERON_0000966\u0026_UBERON], (kidney)[UBERON_0002113\u0026_UBERON], (heart)[UBERON_0000948\u0026_UBERON], and (skin)[UBERON_0000014\u0026UBERON_0002097\u0026_UBERON].",
				"CR were 95.6%25 %252844/46 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the (gallbladder)[UBERON_0002110\u0026_UBERON], 92.9%25%252879/85 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the (diaphragm)[UBERON_0001103\u0026_UBERON], 90.9%25%252840/44 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near the gastrointestinal tract, 91.2%25 %252831/34 (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO]%2529 for (tumors)[HP_0002664\u0026MONDO_0005070\u0026_HP\u0026_MONDO] near large (vessel)[UBERON_0000055\u0026_UBERON].");

		BiolinkAssociation biolinkAssociation = BiolinkAssociation.BL_DISEASE_OR_PHENOTYPIC_FEATURE_TO_LOCATION;
		String batchId = "1";

		List<TextDocument> inputSentences = new ArrayList<TextDocument>();
		Set<String> ontologyPrefixes = new HashSet<String>();
		ontologyPrefixes.addAll(biolinkAssociation.getSubjectClass().getOntologyPrefixes());
		ontologyPrefixes.addAll(biolinkAssociation.getObjectClass().getOntologyPrefixes());
		for (String annotatedText : annotatedTexts) {
			TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
					ontologyPrefixes, null);
			inputSentences.add(td);
		}

		List<TextDocument> redundantSentences = new ArrayList<TextDocument>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		String redundantSentence1 = "Here is the text for the redundant sentence 1.";
		TextDocument td1 = new TextDocument("id", "source", redundantSentence1);
		td1.addAnnotation(factory.createAnnotation(0, 4, "Here", "anatomical_site"));
		td1.addAnnotation(factory.createAnnotation(12, 16, "text", "disease_or_phenotypic_feature"));

		String redundantSentence2 = "Here is the text for the redundant sentence 2.";
		TextDocument td2 = new TextDocument("id", "source", redundantSentence2);
		td2.addAnnotation(factory.createAnnotation(0, 4, "Here", "anatomical_site"));
		td2.addAnnotation(factory.createAnnotation(12, 16, "text", "disease_or_phenotypic_feature"));

		redundantSentences.add(td1);
		redundantSentences.add(td2);

		assertEquals(4, inputSentences.size());
		int batchSize = 5;
		int sentencesPerPage = 5;
		Set<String> identifiersToExclude = new HashSet<String>();
		ElasticsearchToBratExporter.createBratFiles(outputDirectory, biolinkAssociation, batchId, batchSize,
				inputSentences, previousSentenceIdsFile, identifiersToExclude, sentencesPerPage, redundantSentences,
				false, null, 0f);

		File annFile = new File(outputDirectory,
				String.format("%s_%s_0.ann", biolinkAssociation.name().toLowerCase(), batchId));
		File txtFile = new File(outputDirectory,
				String.format("%s_%s_0.txt", biolinkAssociation.name().toLowerCase(), batchId));

		assertTrue(annFile.exists());
		assertTrue(txtFile.exists());

		for (String line : FileReaderUtil.loadLinesFromFile(txtFile, UTF8)) {
			System.out.println(line);
		}

		// redundant sentences should be randomly placed in the output so it's difficult
		// to test for the explicit annotations b/c the ordering is not deterministic,
		// so we will count annotation lines instead

		List<String> txtLines = FileReaderUtil.loadLinesFromFile(txtFile, UTF8);

		assertEquals(6, txtLines.size()); // 5 sentences + "DONE" = 6 lines

		// the 2 redundant sentences should be present
		assertTrue(txtLines.contains(redundantSentence1));
		assertTrue(txtLines.contains(redundantSentence2));

	}

	@Test
	public void testDeserializeAnnotatedText_withAllowedConceptIds() {

		Set<String> ontologyPrefixes = CollectionsUtil.createSet("DRUGBANK");
		String annotatedText = "(Hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q disorders including (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Iran, (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Thailand, and (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-India are important (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] variants.";
		Map<String, Set<String>> prefixToAllowedConceptIdsMap = new HashMap<String, Set<String>>();
		prefixToAllowedConceptIdsMap.put("DRUGBANK", CollectionsUtil.createSet("DRUGBANK:DB04945"));
		TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
				ontologyPrefixes, prefixToAllowedConceptIdsMap);
		// 1 2 3 4 5 6 7 8 9 0 1 2 3
		// 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
		String expectedText = "Hemoglobin Q disorders including hemoglobin Q-Iran, hemoglobin Q-Thailand, and hemoglobin Q-India are important hemoglobin variants.";
		assertEquals(expectedText, td.getText());

		Set<TextAnnotation> expectedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "GO:0005833"));

		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(td.getAnnotations()));

	}

	@Test
	public void testDeserializeAnnotatedText_withAllowedConceptIds2() {

		Set<String> ontologyPrefixes = CollectionsUtil.createSet("DRUGBANK");
		String annotatedText = "(Hemoglobin)[DRUGBANK_DB04945111&GO_0005833&_DRUGBANK&_GO] Q disorders including (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Iran, (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-Thailand, and (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] Q-India are important (hemoglobin)[DRUGBANK_DB04945&GO_0005833&_DRUGBANK&_GO] variants.";
		Map<String, Set<String>> prefixToAllowedConceptIdsMap = new HashMap<String, Set<String>>();
		prefixToAllowedConceptIdsMap.put("DRUGBANK", CollectionsUtil.createSet("DRUGBANK:DB04945"));
		TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText("PMID:1234", annotatedText,
				ontologyPrefixes, prefixToAllowedConceptIdsMap);
		// 1 2 3 4 5 6 7 8 9 0 1 2 3
		// 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
		String expectedText = "Hemoglobin Q disorders including hemoglobin Q-Iran, hemoglobin Q-Thailand, and hemoglobin Q-India are important hemoglobin variants.";
		assertEquals(expectedText, td.getText());

		Set<TextAnnotation> expectedAnnots = new HashSet<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
//		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(0, 10, "Hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(33, 43, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(52, 62, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(79, 89, "hemoglobin", "GO:0005833"));

		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "DRUGBANK:DB04945"));
//		expectedAnnots.add(factory.createAnnotation(112, 122, "hemoglobin", "GO:0005833"));

		assertEquals(expectedAnnots, new HashSet<TextAnnotation>(td.getAnnotations()));

	}

	@Test
	public void testSplitIntoSentences() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		StringBuilder docText = new StringBuilder();
		String sent1 = "Here is the text for the redundant sentence 1.";
		String sent2 = "Here is the text for the redundant sentence 2.";
		String sent3 = "Here is the text for the redundant sentence 3.";
		String sent4 = "Here is the text for the redundant sentence 4.";
		docText.append(sent1 + "\n");
		docText.append(sent2 + "\n");
		docText.append(sent3 + "\n");
		docText.append(sent4 + "\n");

		TextDocument td = new TextDocument("docid", "docsource", docText.toString());

		int offset = 0;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent1.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent2.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent3.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));

		TextAnnotation expectedAnnot1 = factory.createAnnotation(0, 4, "Here", "anatomical_site");
		TextAnnotation expectedAnnot2 = factory.createAnnotation(12, 16, "text", "disease_or_phenotypic_feature");

		List<TextDocument> sentenceDocs = ElasticsearchToBratExporter.splitIntoSentences(td);

		assertEquals(4, sentenceDocs.size());

		TextDocument td1 = sentenceDocs.get(0);
		assertEquals(sent1, td1.getText());
		assertEquals(2, td1.getAnnotations().size());
		assertEquals(expectedAnnot1, td1.getAnnotations().get(0));
		assertEquals(expectedAnnot2, td1.getAnnotations().get(1));

		TextDocument td2 = sentenceDocs.get(1);
		assertEquals(sent2, td2.getText());
		assertEquals(2, td2.getAnnotations().size());
		assertEquals(expectedAnnot1, td2.getAnnotations().get(0));
		assertEquals(expectedAnnot2, td2.getAnnotations().get(1));

		TextDocument td3 = sentenceDocs.get(2);
		assertEquals(sent3, td3.getText());
		assertEquals(2, td3.getAnnotations().size());
		assertEquals(expectedAnnot1, td3.getAnnotations().get(0));
		assertEquals(expectedAnnot2, td3.getAnnotations().get(1));

		TextDocument td4 = sentenceDocs.get(3);
		assertEquals(sent4, td4.getText());
		assertEquals(2, td4.getAnnotations().size());
		assertEquals(expectedAnnot1, td4.getAnnotations().get(0));
		assertEquals(expectedAnnot2, td4.getAnnotations().get(1));

	}

	@Test
	public void testLoadRedundantSentencesForAnnotation() throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		StringBuilder docText = new StringBuilder();
		String sent1 = "Here is the text for the redundant sentence 1.";
		String sent2 = "Here is the text for the redundant sentence 2.";
		String sent3 = "Here is the text for the redundant sentence 3.";
		String sent4 = "Here is the text for the redundant sentence 4.";
		String sent5 = "Here is the text for the redundant sentence 5.";
		String sent6 = "Here is the text for the redundant sentence 6.";
		String sent7 = "Here is the text for the redundant sentence 7.";
		String sent8 = "Here is the text for the redundant sentence 8.";
		String sent9 = "Here is the text for the redundant sentence 9.";
		String sent10 = "Here is the text for the redundant sentence 10.";
		docText.append(sent1 + "\n");
		docText.append(sent2 + "\n");
		docText.append(sent3 + "\n");
		docText.append(sent4 + "\n");
		docText.append(sent5 + "\n");
		docText.append(sent6 + "\n");
		docText.append(sent7 + "\n");
		docText.append(sent8 + "\n");
		docText.append(sent9 + "\n");
		docText.append(sent10 + "\n");

		TextDocument td = new TextDocument("docid", "docsource", docText.toString());

		int offset = 0;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent1.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent2.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent3.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent4.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent5.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent6.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent7.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent8.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));
		offset += sent9.length() + 1;
		td.addAnnotation(factory.createAnnotation(0 + offset, 4 + offset, "Here", "anatomical_site"));
		td.addAnnotation(factory.createAnnotation(12 + offset, 16 + offset, "text", "disease_or_phenotypic_feature"));

		File dir = folder.newFolder();
		File annFile = new File(dir, "batch.ann");
		File txtFile = new File(dir, "batch.txt");

		File cachedIdsFile = folder.newFile();

		FileWriterUtil.printLines(Arrays.asList(td.getText()), txtFile, UTF8);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		writer.serialize(td, annFile, UTF8);

		float batchOverlapPercentage = 0.2f;
		List<TextDocument> redundantSentences = ElasticsearchToBratExporter
				.loadRedundantSentencesForAnnotation(batchOverlapPercentage, annFile, cachedIdsFile);
		assertEquals(2, redundantSentences.size());

		batchOverlapPercentage = 0.3f;
		redundantSentences = ElasticsearchToBratExporter.loadRedundantSentencesForAnnotation(batchOverlapPercentage,
				annFile, cachedIdsFile);
		assertEquals(3, redundantSentences.size());

		batchOverlapPercentage = 0.5f;
		redundantSentences = ElasticsearchToBratExporter.loadRedundantSentencesForAnnotation(batchOverlapPercentage,
				annFile, cachedIdsFile);
		assertEquals(5, redundantSentences.size());

		batchOverlapPercentage = 0.8f;
		redundantSentences = ElasticsearchToBratExporter.loadRedundantSentencesForAnnotation(batchOverlapPercentage,
				annFile, cachedIdsFile);
		assertEquals(8, redundantSentences.size());

		batchOverlapPercentage = 1.0f;
		redundantSentences = ElasticsearchToBratExporter.loadRedundantSentencesForAnnotation(batchOverlapPercentage,
				annFile, cachedIdsFile);
		assertEquals(10, redundantSentences.size());

	}

}
