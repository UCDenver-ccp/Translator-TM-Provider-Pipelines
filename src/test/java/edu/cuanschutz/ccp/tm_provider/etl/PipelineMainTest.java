package edu.cuanschutz.ccp.tm_provider.etl;

import static edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil.createEntity;
import static edu.cuanschutz.ccp.tm_provider.etl.PipelineTestUtil.createProcessingStatus;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.CrfOrConcept;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationUtil;
import lombok.Data;

public class PipelineMainTest {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testChunkString() throws UnsupportedEncodingException {
		StringBuffer s = new StringBuffer();

		for (int i = 0; i < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES; i++) {
			s.append("a");
		}

		assertEquals("equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// add one more character and it should now return 2 strings
		s.append("b");
		assertEquals("equal to the threshold, so should be two strings", 2,
				PipelineMain.chunkContent(s.toString()).size());

		// remove two characters (in this case the first and second) and it should
		// return to 1 string
		s = s.deleteCharAt(0);
		s = s.deleteCharAt(0);
		assertEquals("back to equal to the threshold, so should just be one string", 1,
				PipelineMain.chunkContent(s.toString()).size());

		// we are now at one short of the threshold. Add a UTF-8 char that is multi-byte
		// and it should jump to two strings
		s.append("\u0190");
		assertEquals(
				"equal to the threshold in character count, but greater than the byte count threshold, so should be two strings",
				2, PipelineMain.chunkContent(s.toString()).size());

	}

	@Data
	private static class PCollectionCountCheckerFn<T> implements SerializableFunction<Iterable<T>, Void> {

		private static final long serialVersionUID = 1L;
		private final int expectedCount;

		@Override
		public Void apply(Iterable<T> input) {
			long count = StreamSupport.stream(input.spliterator(), false).count();
			assertEquals(String.format("There should only be %d items in the PCollection", expectedCount),
					expectedCount, count);
			return null;
		}

	}

	@Test
	public void testDeduplicateDocumentsStringKey() {

		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

		List<KV<String, List<String>>> input = Arrays.asList(KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId2, Arrays.asList("doc2 text")), KV.of(docId3, Arrays.asList("doc3 text")),
				KV.of(docId4, Arrays.asList("doc4 text")), KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId1, Arrays.asList("doc1 text")), KV.of(docId2, Arrays.asList("doc2 text")),
				KV.of(docId2, Arrays.asList("doc2 text")));

		assertEquals("there should only be 4 unique entries in the input", 4,
				new HashSet<KV<String, List<String>>>(input).size());

		PCollection<KV<String, List<String>>> statusEntityToDocContent = pipeline.apply(Create.of(input));
		PCollection<KV<String, List<String>>> nonredundantStatusEntityToDocContent = PipelineMain
				.deduplicateDocumentsByStringKey(statusEntityToDocContent);

		PAssert.that(nonredundantStatusEntityToDocContent)
				.satisfies(new PCollectionCountCheckerFn<KV<String, List<String>>>(4));

		pipeline.run();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDeduplicateDocuments() {

		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

//		Entity entity1 = createEntity(docId1, ProcessingStatusFlag.TEXT_DONE);
//		Entity entity2 = createEntity(docId2, ProcessingStatusFlag.TEXT_DONE);
//		Entity entity3 = createEntity(docId3, ProcessingStatusFlag.TEXT_DONE);
//		Entity entity4 = createEntity(docId4, ProcessingStatusFlag.TEXT_DONE);

		ProcessingStatus entity1 = createProcessingStatus(docId1, ProcessingStatusFlag.TEXT_DONE);
		ProcessingStatus entity2 = createProcessingStatus(docId2, ProcessingStatusFlag.TEXT_DONE);
		ProcessingStatus entity3 = createProcessingStatus(docId3, ProcessingStatusFlag.TEXT_DONE);
		ProcessingStatus entity4 = createProcessingStatus(docId4, ProcessingStatusFlag.TEXT_DONE);

		List<KV<ProcessingStatus, List<String>>> input = Arrays.asList(KV.of(entity1, Arrays.asList("doc1 text")),
				KV.of(entity2, Arrays.asList("doc2 text")), KV.of(entity3, Arrays.asList("doc3 text")),
				KV.of(entity4, Arrays.asList("doc4 text")), KV.of(entity1, Arrays.asList("doc1 text")),
				KV.of(entity1, Arrays.asList("doc1 text")), KV.of(entity2, Arrays.asList("doc2 text")),
				KV.of(entity2, Arrays.asList("doc2 text")));

		assertEquals("there should only be 4 unique entries in the input", 4,
				new HashSet<KV<ProcessingStatus, List<String>>>(input).size());

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToDocContent = pipeline.apply(Create.of(input));
		PCollection<KV<String, List<String>>> nonredundantStatusEntityToDocContent = PipelineMain
				.deduplicateDocuments(statusEntityToDocContent);

		PAssert.that(nonredundantStatusEntityToDocContent)
				.satisfies(new PCollectionCountCheckerFn<KV<String, List<String>>>(4));

		PAssert.that(nonredundantStatusEntityToDocContent).containsInAnyOrder(KV.of(docId1, Arrays.asList("doc1 text")),
				KV.of(docId2, Arrays.asList("doc2 text")), KV.of(docId3, Arrays.asList("doc3 text")),
				KV.of(docId4, Arrays.asList("doc4 text")));

		pipeline.run();
	}

	@Test
	public void testDeduplicateStatusEnititie() {
		String docId1 = "PMID:1";
		String docId2 = "PMID:2";
		String docId3 = "PMID:3";
		String docId4 = "PMID:4";

		Entity entity1 = createEntity(docId1, ProcessingStatusFlag.TEXT_DONE);
		Entity entity2 = createEntity(docId2, ProcessingStatusFlag.TEXT_DONE);
		Entity entity3 = createEntity(docId3, ProcessingStatusFlag.TEXT_DONE);
		Entity entity4 = createEntity(docId4, ProcessingStatusFlag.TEXT_DONE);

		List<Entity> input = Arrays.asList(entity1, entity2, entity3, entity4, entity1, entity3, entity4);

		assertEquals("there should only be 4 unique entries in the input", 4, new HashSet<Entity>(input).size());

		PCollection<Entity> statusEntities = pipeline.apply(Create.of(input));

		PCollection<Entity> nonredundantEntities = PipelineMain.deduplicateStatusEntities(statusEntities);

		PAssert.that(nonredundantEntities).satisfies(new PCollectionCountCheckerFn<Entity>(4));

		PAssert.that(nonredundantEntities).containsInAnyOrder(entity1, entity2, entity3, entity4);

		pipeline.run();

	}

	@Test
	public void testSpliceDocumentChunks() {

		String documentId = "PMID:12345";

		DocumentCriteria documentCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");

		String documentContent1 = "The content of the first chunk11111111111.";
		String documentContent2 = "The content of the second chunk2222222222.";
		String documentContent3 = "The content of the third chunk33333333333.";

		long chunkTotal = 3;

		Set<String> collections = CollectionsUtil.createSet("PUBMED");

		ProcessedDocument pd1 = new ProcessedDocument(documentId, documentCriteria, documentContent1, 0, chunkTotal,
				collections);
		ProcessedDocument pd2 = new ProcessedDocument(documentId, documentCriteria, documentContent2, 1, chunkTotal,
				collections);
		ProcessedDocument pd3 = new ProcessedDocument(documentId, documentCriteria, documentContent3, 2, chunkTotal,
				collections);

		Map<DocumentCriteria, String> result = PipelineMain.spliceDocumentChunks(Arrays.asList(pd1, pd2, pd3));

		Map<DocumentCriteria, String> expectedResult = new HashMap<DocumentCriteria, String>();
		expectedResult.put(documentCriteria, documentContent1 + documentContent2 + documentContent3);

		assertEquals(expectedResult, result);
	}

	@Test
	public void testSpliceDocumentChunksWithMultipleDocTypes() {

		String documentId = "PMID:12345";

		DocumentCriteria documentCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria documentCriteria4 = new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP,
				PipelineKey.OGER, "0.1.0");

		String documentContent1 = "The content of the first chunk11111111111.";
		String documentContent2 = "The content of the second chunk2222222222.";
		String documentContent3 = "The content of the third chunk33333333333.";
		String documentContent4 = "The content of the third chunk44444444444.";

		long chunkTotal = 3;

		Set<String> collections = CollectionsUtil.createSet("PUBMED");

		ProcessedDocument pd1 = new ProcessedDocument(documentId, documentCriteria, documentContent1, 0, chunkTotal,
				collections);
		ProcessedDocument pd2 = new ProcessedDocument(documentId, documentCriteria, documentContent2, 1, chunkTotal,
				collections);
		ProcessedDocument pd3 = new ProcessedDocument(documentId, documentCriteria, documentContent3, 2, chunkTotal,
				collections);
		ProcessedDocument pd4 = new ProcessedDocument(documentId, documentCriteria4, documentContent4, 0, 1,
				collections);

		Map<DocumentCriteria, String> result = PipelineMain.spliceDocumentChunks(Arrays.asList(pd3, pd1, pd4, pd2));

		Map<DocumentCriteria, String> expectedResult = new HashMap<DocumentCriteria, String>();
		expectedResult.put(documentCriteria, documentContent1 + documentContent2 + documentContent3);
		expectedResult.put(documentCriteria4, documentContent4);

		assertEquals(expectedResult, result);
	}

	@Test
	public void testCompileInputDocumentCriteria() {
		String s = "TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_CHEBI|BIONLP|OGER|0.1.0";
		Set<DocumentCriteria> docCriteria = PipelineMain.compileInputDocumentCriteria(s);

		Set<DocumentCriteria> expectedDocCriteria = CollectionsUtil.createSet(
				new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0"),
				new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP, PipelineKey.OGER, "0.1.0"));

		assertEquals(expectedDocCriteria, docCriteria);
	}

	@Test
	public void testCloneTextAnnotation() {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");
		TextAnnotation annot = factory.createAnnotation(0, 5, "some text", "PR:00000000025");
		TextAnnotation clone = PipelineMain.clone(annot);
		assertEquals(annot, clone);

		// test with multiple spans
		annot.addSpan(new Span(15, 20));
		clone = PipelineMain.clone(annot);

		assertEquals(annot, clone);
	}

	@Test
	public void testPairConceptWithCrfAnnots() throws IOException {
		Map<DocumentType, Collection<TextAnnotation>> inputMap = loadDocumentTypeToAnnotMap();

		Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> pairConceptWithCrfAnnots = PipelineMain
				.pairConceptWithCrfAnnots(inputMap);

		Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> expectedPairConceptWithCrfAnnots = new HashMap<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>>();

		Map<CrfOrConcept, Set<TextAnnotation>> prMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> mondoMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> goBpMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> soMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> ncbiTaxonMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> hpMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> snomedMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
		Map<CrfOrConcept, Set<TextAnnotation>> uberonMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();

		prMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T1	PR:000004804 0 5	BRCA2"),
						buildAnnot("T2	PR:000004804 147 152	BRCA2"), buildAnnot("T4	PR:000004804 233 238	BRCA2"),
						buildAnnot("T5	PR:000004804 396 401	BRCA2"), buildAnnot("T6	PR:000004804 452 457	BRCA2"),
						buildAnnot("T8	PR:000013672 550 555	RAD51"), buildAnnot("T9	PR:000017505 550 555	RAD51"),
						buildAnnot("T10	PR:000004804 733 738	BRCA2"),
						buildAnnot("T12	PR:000013672 742 747	RAD51"),
						buildAnnot("T13	PR:000017505 742 747	RAD51"))));

		mondoMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T2	MONDO:0007254 113 126	breast cancer"),
						buildAnnot("T10	MONDO:0007254 776 789	breast cancer"))));

		goBpMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T3	GO_BP:0006281 156 166	DNA repair"),
						buildAnnot("T1	GO_BP:0035825 10 34	homologous recombination"),
						buildAnnot("T3	GO_BP:0000725 428 450	recombinational repair"),
						buildAnnot("T5	GO_BP:0035825 791 815	homologous recombination"))));

		hpMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T7	HP:0033567 550 553	RAD"),
						buildAnnot("T11	HP:0033567 742 745	RAD"), buildAnnot("T1	HP:0003002 113 126	breast cancer"),
						buildAnnot("T9	HP:0003002 776 789	breast cancer"))));

		soMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T3	SO:0000704 142 146	gene"))));

		snomedMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T4	SNOMEDCT:4365001 160 166	repair"),
						buildAnnot("T5	SNOMEDCT:4365001 279 285	repair"),
						buildAnnot("T6	SNOMEDCT:4365001 444 450	repair"),
						buildAnnot("T8	SNOMEDCT:4365001 757 763	repair"))));

		ncbiTaxonMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T7	NCBITaxon:1925465 521 526	major"))));

		uberonMap.put(CrfOrConcept.CONCEPT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T2	UBERON:0000310 113 119	breast"),
						buildAnnot("T4	UBERON:0000310 776 782	breast"))));

		mondoMap.put(CrfOrConcept.CRF,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T0	ENTITY 113 126	breast cancer"),
						buildAnnot("T1	ENTITY 776 789	breast cancer"), buildAnnot("T1	B-MONDO 113 119	breast"),
						buildAnnot("T2	I-MONDO 120 126	cancer"), buildAnnot("T18	B-MONDO 776 782	breast"),
						buildAnnot("T19	I-MONDO 783 789	cancer"))));

		hpMap.put(CrfOrConcept.CRF,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T0	ENTITY 113 126	breast cancer"),
						buildAnnot("T1	ENTITY 776 789	breast cancer"), buildAnnot("T1	B-MONDO 113 119	breast"),
						buildAnnot("T2	I-MONDO 120 126	cancer"), buildAnnot("T18	B-MONDO 776 782	breast"),
						buildAnnot("T19	I-MONDO 783 789	cancer"))));

		prMap.put(CrfOrConcept.CRF,
				new HashSet<TextAnnotation>(
						Arrays.asList(buildAnnot("T0	B-PR 0 5	BRCA2"), buildAnnot("T4	B-PR 147 152	BRCA2"),
								buildAnnot("T8	B-PR 233 238	BRCA2"), buildAnnot("T9	B-PR 396 401	BRCA2"),
								buildAnnot("T12	B-PR 452 457	BRCA2"), buildAnnot("T14	B-PR 550 555	RAD51"),
								buildAnnot("T15	B-PR 733 738	BRCA2"), buildAnnot("T16	B-PR 742 747	RAD51"))));

		soMap.put(CrfOrConcept.CRF, new HashSet<TextAnnotation>(
				Arrays.asList(buildAnnot("T3	B-SO 142 146	gene"), buildAnnot("T7	B-SO 191 198	genetic"))));

		goBpMap.put(CrfOrConcept.CRF,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T5	B-GO_BP 156 159	DNA"),
						buildAnnot("T6	I-GO_BP 160 166	repair"), buildAnnot("T10	B-GO_BP 428 443	recombinational"),
						buildAnnot("T11	I-GO_BP 444 450	repair"), buildAnnot("T17	B-GO_BP 757 763	repair"))));

		ncbiTaxonMap.put(CrfOrConcept.CRF,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T13	B-NCBITaxon 527 537	eukaryotic"))));

		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_PR, prMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_MONDO, mondoMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_GO_BP, goBpMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_SO, soMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_NCBITAXON, ncbiTaxonMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_HP, hpMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_SNOMEDCT, snomedMap);
		expectedPairConceptWithCrfAnnots.put(DocumentType.CONCEPT_UBERON, uberonMap);

		assertEquals(expectedPairConceptWithCrfAnnots.size(), pairConceptWithCrfAnnots.size());
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_NCBITAXON),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_NCBITAXON));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_SO),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_SO));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_HP),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_HP));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_PR),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_PR));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_MONDO),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_MONDO));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_GO_BP),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_GO_BP));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_SNOMEDCT),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_SNOMEDCT));
		assertEquals(expectedPairConceptWithCrfAnnots.get(DocumentType.CONCEPT_UBERON),
				pairConceptWithCrfAnnots.get(DocumentType.CONCEPT_UBERON));
		assertEquals(expectedPairConceptWithCrfAnnots, pairConceptWithCrfAnnots);

	}

	private Map<DocumentType, Collection<TextAnnotation>> loadDocumentTypeToAnnotMap() throws IOException {
		String docText = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.txt", ENCODING);
		String craftCrfBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.craft_crf.bionlp",
				ENCODING);
		String nlmDiseaseCrfBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(),
				"12345.nlmdisease_crf.bionlp", ENCODING);
		String ogerCimaxBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.oger_cimax.bionlp",
				ENCODING);
		String ogerCiminBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.oger_cimin.bionlp",
				ENCODING);
		String ogerCsBionlp = ClassPathUtil.getContentsFromClasspathResource(getClass(), "12345.oger_cs.bionlp",
				ENCODING);

		BioNLPDocumentReader bionlpDocReader = new BioNLPDocumentReader();
		TextDocument craftCrfTd = bionlpDocReader.readDocument("12345", "pmid",
				new ByteArrayInputStream(craftCrfBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument nlmDiseaseCrfTd = bionlpDocReader.readDocument("12345", "pmid",
				new ByteArrayInputStream(nlmDiseaseCrfBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument ogerCimaxTd = bionlpDocReader.readDocument("12345", "pmid",
				new ByteArrayInputStream(ogerCimaxBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument ogerCiminTd = bionlpDocReader.readDocument("12345", "pmid",
				new ByteArrayInputStream(ogerCiminBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);
		TextDocument ogerCsTd = bionlpDocReader.readDocument("12345", "pmid",
				new ByteArrayInputStream(ogerCsBionlp.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				ENCODING);

		/*
		 * The input map will have concept annotations from the CS MIMIN and CIMAX
		 * documents from OGER, and from the CRF_CRAFT and CRF_DISEASE documents from
		 * the CRF services.
		 */
		Map<DocumentType, Collection<TextAnnotation>> inputMap = new HashMap<DocumentType, Collection<TextAnnotation>>();
		inputMap.put(DocumentType.CONCEPT_CS, ogerCsTd.getAnnotations());
		inputMap.put(DocumentType.CONCEPT_CIMAX, ogerCimaxTd.getAnnotations());
		inputMap.put(DocumentType.CONCEPT_CIMIN, ogerCiminTd.getAnnotations());
		inputMap.put(DocumentType.CRF_CRAFT, craftCrfTd.getAnnotations());
		inputMap.put(DocumentType.CRF_NLMDISEASE, nlmDiseaseCrfTd.getAnnotations());

		return inputMap;
	}

	/**
	 * builds an annotation from a bionlp formatted string
	 * 
	 * @param s
	 * @return
	 */
	private TextAnnotation buildAnnot(String s) {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("12345");
		String[] cols = s.split("\\t");
		String tIndex = cols[0];
		String[] idSpan = cols[1].split(" ");
		String id = idSpan[0];
		int spanStart = Integer.parseInt(idSpan[1]);
		int spanEnd = Integer.parseInt(idSpan[2]);
		String covered = cols[2];

		TextAnnotation annot = factory.createAnnotation(spanStart, spanEnd, covered, id);
		TextAnnotationUtil.addSlotValue(annot, "theme id", tIndex);

		return annot;

	}

	@Test
	public void testFilterViaCrf() {

		Collection<TextAnnotation> conceptAnnots = new HashSet<TextAnnotation>(Arrays.asList(
				buildAnnot("T1	PR:000004804 0 5	BRCA2"), buildAnnot("T2	PR:000004804 147 152	BRCA2"),
				buildAnnot("T4	PR:000004804 233 238	BRCA2"), buildAnnot("T5	PR:000004804 396 401	BRCA2"),
				buildAnnot("T6	PR:000004804 452 457	BRCA2"), buildAnnot("T8	PR:000013672 550 555	RAD51"),
				buildAnnot("T9	PR:000017505 550 555	RAD51"), buildAnnot("T10	PR:000004804 733 738	BRCA2"),
				buildAnnot("T12	PR:000013672 742 747	RAD51"), buildAnnot("T13	PR:000017505 742 747	RAD51")));
		Collection<TextAnnotation> crfAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(buildAnnot("T0	B-PR 0 5	BRCA2"), buildAnnot("T4	B-PR 147 152	BRCA2"),
						buildAnnot("T8	B-PR 233 238	BRCA2"), buildAnnot("T9	B-PR 396 401	BRCA2"),
						buildAnnot("T12	B-PR 452 457	BRCA2"), buildAnnot("T15	B-PR 733 738	BRCA2")));

		Set<TextAnnotation> filteredConceptAnnots = PipelineMain.filterViaCrf(conceptAnnots, crfAnnots);

		Set<TextAnnotation> expectedFilteredConceptAnnots = new HashSet<TextAnnotation>(Arrays.asList(
				buildAnnot("T1	PR:000004804 0 5	BRCA2"), buildAnnot("T2	PR:000004804 147 152	BRCA2"),
				buildAnnot("T4	PR:000004804 233 238	BRCA2"), buildAnnot("T5	PR:000004804 396 401	BRCA2"),
				buildAnnot("T6	PR:000004804 452 457	BRCA2"), buildAnnot("T10	PR:000004804 733 738	BRCA2")));

		assertEquals("The RAD51 concepts should be filtered.", expectedFilteredConceptAnnots, filteredConceptAnnots);

	}

	@Test
	public void testFilterConceptAnnotations() throws IOException {
		Map<DocumentType, Collection<TextAnnotation>> inputMap = loadDocumentTypeToAnnotMap();
		Map<DocumentType, Set<TextAnnotation>> filteredConceptAnnots = PipelineMain.filterConceptAnnotations(inputMap,
				FilterFlag.BY_CRF);

		Map<DocumentType, Set<TextAnnotation>> expectedFilteredConceptAnnots = new HashMap<DocumentType, Set<TextAnnotation>>();

		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_PR,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T1	PR:000004804 0 5	BRCA2"),
						buildAnnot("T2	PR:000004804 147 152	BRCA2"), buildAnnot("T4	PR:000004804 233 238	BRCA2"),
						buildAnnot("T5	PR:000004804 396 401	BRCA2"), buildAnnot("T6	PR:000004804 452 457	BRCA2"),
						buildAnnot("T8	PR:000013672 550 555	RAD51"), buildAnnot("T9	PR:000017505 550 555	RAD51"),
						buildAnnot("T10	PR:000004804 733 738	BRCA2"),
						buildAnnot("T12	PR:000013672 742 747	RAD51"),
						buildAnnot("T13	PR:000017505 742 747	RAD51"))));
		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_MONDO,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T2	MONDO:0007254 113 126	breast cancer"),
						buildAnnot("T10	MONDO:0007254 776 789	breast cancer"))));
		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_GO_BP,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T3	GO_BP:0006281 156 166	DNA repair"),
//						buildAnnot("T1	GO_BP:0035825 10 34	homologous recombination"),
						buildAnnot("T3	GO_BP:0000725 428 450	recombinational repair")
//						buildAnnot("T5	GO_BP:0035825 791 815	homologous recombination")
				)));
		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_SO,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T3	SO:0000704 142 146	gene"))));
//		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_NCBITAXON,
//				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T7	NCBITaxon:1925465 521 526	major"))));
		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_HP, new HashSet<TextAnnotation>(Arrays.asList(
//						buildAnnot("T7	HP:0033567 550 553	RAD"),
//						buildAnnot("T11	HP:0033567 742 745	RAD"), 
				buildAnnot("T1	HP:0003002 113 126	breast cancer"),
				buildAnnot("T9	HP:0003002 776 789	breast cancer"))));
		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_SNOMEDCT,
				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T4	SNOMEDCT:4365001 160 166	repair"),
						buildAnnot("T5	SNOMEDCT:4365001 279 285	repair"),
						buildAnnot("T6	SNOMEDCT:4365001 444 450	repair"),
						buildAnnot("T8	SNOMEDCT:4365001 757 763	repair"))));
//		expectedFilteredConceptAnnots.put(DocumentType.CONCEPT_UBERON,
//				new HashSet<TextAnnotation>(Arrays.asList(buildAnnot("T2	UBERON:0000310 113 119	breast"),
//						buildAnnot("T4	UBERON:0000310 776 782	breast"))));

		assertEquals(expectedFilteredConceptAnnots.keySet(), filteredConceptAnnots.keySet());
		assertEquals(expectedFilteredConceptAnnots.size(), filteredConceptAnnots.size());

		assertEquals(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_MONDO),
				filteredConceptAnnots.get(DocumentType.CONCEPT_MONDO));
		assertEquals(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_GO_BP),
				filteredConceptAnnots.get(DocumentType.CONCEPT_GO_BP));
		assertEquals(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_SO),
				filteredConceptAnnots.get(DocumentType.CONCEPT_SO));
		assertEquals(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_SNOMEDCT),
				filteredConceptAnnots.get(DocumentType.CONCEPT_SNOMEDCT));
		assertEquals(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_HP),
				filteredConceptAnnots.get(DocumentType.CONCEPT_HP));
		assertNull(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_NCBITAXON));
		assertNull(expectedFilteredConceptAnnots.get(DocumentType.CONCEPT_UBERON));

		assertEquals(expectedFilteredConceptAnnots, filteredConceptAnnots);

	}

	@Test
	public void testFilterForMostRecent() {
		Map<DocumentCriteria, String> contentMap = new HashMap<DocumentCriteria, String>();
		DocumentCriteria dc1 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.1.1");
		DocumentCriteria dc2 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.1.2");
		DocumentCriteria dc3 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.3.1");
		DocumentCriteria dc4 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"1.1.1");

		DocumentCriteria dc5 = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.0");
		DocumentCriteria dc6 = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.1");

		DocumentCriteria dc7 = new DocumentCriteria(DocumentType.ABBREVIATIONS, DocumentFormat.BIONLP,
				PipelineKey.ABBREVIATION, "0.3.0");

		contentMap.put(dc1, "old");
		contentMap.put(dc2, "old");
		contentMap.put(dc3, "old");
		contentMap.put(dc4, "recent");
		contentMap.put(dc5, "old");
		contentMap.put(dc6, "recent");
		contentMap.put(dc7, "recent");

		Map<DocumentCriteria, String> filteredContentMap = PipelineMain.filterForMostRecent(contentMap);

		Map<DocumentCriteria, String> expectedFilteredContentMap = new HashMap<DocumentCriteria, String>();
		expectedFilteredContentMap.put(dc4, "recent");
		expectedFilteredContentMap.put(dc6, "recent");
		expectedFilteredContentMap.put(dc7, "recent");

		assertEquals(expectedFilteredContentMap, filteredContentMap);
	}

	@Test
	public void testGetMostRecent() {
		Map<DocumentCriteria, String> map = new HashMap<DocumentCriteria, String>();
		DocumentCriteria dc1 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.1.1");
		DocumentCriteria dc2 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.1.2");
		DocumentCriteria dc3 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"0.3.1");
		DocumentCriteria dc4 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"1.1.1");

		map.put(dc1, "old");
		map.put(dc2, "old");
		map.put(dc3, "old");
		map.put(dc4, "recent");

		KV<DocumentCriteria, String> mostRecent = PipelineMain.getMostRecent(map);
		KV<DocumentCriteria, String> expectedMostRecent = KV.of(dc4, "recent");
		assertEquals(expectedMostRecent, mostRecent);
	}

	@Test
	public void testIsMoreRecent() {
		assertTrue(PipelineMain.isMoreRecent("1.3.4", "1.3.3"));
		assertTrue(PipelineMain.isMoreRecent("1.2", "1.0.1"));
		assertTrue(PipelineMain.isMoreRecent("5", "4.3.3"));
		assertTrue(PipelineMain.isMoreRecent("1.1.1000", "1.1.999"));
		assertFalse(PipelineMain.isMoreRecent("1.3.3", "1.3.4"));
		assertFalse(PipelineMain.isMoreRecent("4", "10"));
	}

	@Test
	public void testGetSemanticVersion() {
		assertArrayEquals(new int[] { 1, 3, 4 }, PipelineMain.getSemanticVersion("1.3.4"));
		assertArrayEquals(new int[] { 1, 3, 0 }, PipelineMain.getSemanticVersion("1.3"));
		assertArrayEquals(new int[] { 1, 0, 0 }, PipelineMain.getSemanticVersion("1"));
	}

	@Test(expected = java.lang.NumberFormatException.class)
	public void testGetSemanticVersion_e1() {
		assertArrayEquals(new int[] { 1, 3, 4 }, PipelineMain.getSemanticVersion("recent"));
	}

	@Test(expected = java.lang.NumberFormatException.class)
	public void testGetSemanticVersion_e2() {
		assertArrayEquals(new int[] { 1, 3, 4 }, PipelineMain.getSemanticVersion(".3.4"));
	}

	@Test
	public void testFulfillsRequiredDocumentCriteria() {

		Set<DocumentCriteria> requiredDocumentCriteria = new HashSet<DocumentCriteria>();

		DocumentCriteria dc1 = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				PipelineMain.PIPELINE_VERSION_RECENT);
		DocumentCriteria dc2 = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.1");
		DocumentCriteria dc3 = new DocumentCriteria(DocumentType.ABBREVIATIONS, DocumentFormat.BIONLP,
				PipelineKey.ABBREVIATION, PipelineMain.PIPELINE_VERSION_RECENT);

		requiredDocumentCriteria.add(dc1);
		requiredDocumentCriteria.add(dc2);
		requiredDocumentCriteria.add(dc3);

		Set<DocumentCriteria> docCriteria = new HashSet<DocumentCriteria>();

		DocumentCriteria dc1a = new DocumentCriteria(DocumentType.CRF_CRAFT, DocumentFormat.BIONLP, PipelineKey.CRF,
				"1.1.1");
		DocumentCriteria dc2a = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.1");
		DocumentCriteria dc3a = new DocumentCriteria(DocumentType.ABBREVIATIONS, DocumentFormat.BIONLP,
				PipelineKey.ABBREVIATION, "0.3.0");
		docCriteria.add(dc1a);
		docCriteria.add(dc2a);
		docCriteria.add(dc3a);

		assertTrue(PipelineMain.fulfillsRequiredDocumentCriteria(docCriteria, requiredDocumentCriteria));

		docCriteria = new HashSet<DocumentCriteria>();

		DocumentCriteria dc2b = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PipelineKey.MEDLINE_XML_TO_TEXT, "0.1.2");
		docCriteria.add(dc1a);
		docCriteria.add(dc2b); // version mismatch
		docCriteria.add(dc3a);

		assertFalse(PipelineMain.fulfillsRequiredDocumentCriteria(docCriteria, requiredDocumentCriteria));

	}

}
