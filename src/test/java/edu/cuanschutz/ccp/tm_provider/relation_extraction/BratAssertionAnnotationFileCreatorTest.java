package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.BratAssertionAnnotationFileCreator.SentenceIterator;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

public class BratAssertionAnnotationFileCreatorTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private List<String> sampleSentenceLines = CollectionsUtil.createList(
			"bc298d5ad4b8efd1e98ae1f599f7f3a80da29dc5b6d61a95c1a6bdf44f74e17d	Mutation in the @GENE$ gene in familial @DISEASE$	PMC7876816	tau	PR:P19332|PR:P10637|PR:000024142|PR:000010173	16|19	multiple system tauopathy with presenile dementia	MONDO:0017276	37|86		86		Mutation in the tau gene in familial multiple system tauopathy with presenile dementia	REF		2155",
			"a7f207ade80cb1926bd0863a8938d96576408ca019deb9c935d136842ae3bb7d	@DISEASE$ due to P301L @GENE$ mutation showing apathy and severe frontal atrophy but lacking other behavioral changes: a case report and literature review	PMC7876816	Frontotemporal lobar degeneration	MONDO:0017276	0|33	tau	PR:000010173|PR:P19332|PR:000024142|PR:P10637	47|50		175		Frontotemporal lobar degeneration due to P301L tau mutation showing apathy and severe frontal atrophy but lacking other behavioral changes: a case report and literature review	REF		2155",
			"27c44fe4b808ea8f4ec3aec711ef9a09f6aa7bfd1e07016779331e6dcc14d18a	Approximately a third of all @DISEASE$ is genetic, with the first described cause being mutations in the microtubule-associated @GENE$ tau (MAPT) gene in 1998.	PMC7876816	FTD	MONDO:0017276	29|32	protein	PR:000000001	122|129		154		Approximately a third of all FTD is genetic, with the first described cause being mutations in the microtubule-associated protein tau (MAPT) gene in 1998.	INTRO		2155",
			"301bc8f2862afb833dd9c00919405278432afc92e168f71464449cc8781a4942	Approximately a third of all @DISEASE$ is genetic, with the first described cause being mutations in the microtubule-associated protein tau (@GENE$) gene in 1998.	PMC7876816	FTD	MONDO:0017276	29|32	MAPT	PR:000010173	135|139		154		Approximately a third of all FTD is genetic, with the first described cause being mutations in the microtubule-associated protein tau (MAPT) gene in 1998.	INTRO		2155",
			"9ee7342974ea6ea1d33362a658973db56fbdea7ff43bab51d0b6b9d8c6180340	Approximately a third of all @DISEASE$ is genetic, with the first described cause being mutations in the @GENE$ (MAPT) gene in 1998.	PMC7876816	FTD	MONDO:0017276	29|32	microtubule-associated protein tau	PR:000010173	99|133		154		Approximately a third of all FTD is genetic, with the first described cause being mutations in the microtubule-associated protein tau (MAPT) gene in 1998.	INTRO		2155",
			"e6efe5282a9f0e378011ea4a18aee41ed040e78d8e8aed34dad0d89f648e264d	Similarly, for anxiety we employed total scores on the @GENE$ and standard categorisations (0 to 4 no anxiety, 5 to 9 mild anxiety, 10 to 14 moderate anxiety and 15 to 21 severe @DISEASE$ ).	PMC7927208	GAD7	PR:P52890	55|59	anxiety	MONDO:0011918|MONDO:0005618	176|183		186		Similarly, for anxiety we employed total scores on the GAD7 and standard categorisations (0 to 4 no anxiety, 5 to 9 mild anxiety, 10 to 14 moderate anxiety and 15 to 21 severe anxiety ).	METHODS		2155");

	@Test
	public void testGetSubBatchId() {
		assertEquals("aaa", BratAssertionAnnotationFileCreator.getSubBatchId(0));
		assertEquals("aab", BratAssertionAnnotationFileCreator.getSubBatchId(1));
		assertEquals("aac", BratAssertionAnnotationFileCreator.getSubBatchId(2));
		assertEquals("aad", BratAssertionAnnotationFileCreator.getSubBatchId(3));
		assertEquals("aaz", BratAssertionAnnotationFileCreator.getSubBatchId(25));
		assertEquals("aba", BratAssertionAnnotationFileCreator.getSubBatchId(26));
		assertEquals("abb", BratAssertionAnnotationFileCreator.getSubBatchId(27));
		assertEquals("azz", BratAssertionAnnotationFileCreator.getSubBatchId(675));
		assertEquals("baa", BratAssertionAnnotationFileCreator.getSubBatchId(676));
		assertEquals("zzz", BratAssertionAnnotationFileCreator.getSubBatchId(17575));
	}

	@Test
	public void testBratFileGeneration() throws IOException {
		BiolinkAssociation blAssociation = BiolinkAssociation.BL_GENE_TO_DISEASE;
		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		File previousSentenceIdsFile = folder.newFile();
		File sentenceFile = folder.newFile();
		FileWriterUtil.printLines(sampleSentenceLines, sentenceFile, encoding);

		File outputDir = folder.newFolder();
		BratAssertionAnnotationFileCreator brat = new BratAssertionAnnotationFileCreator(outputDir);
		String batchId = "22";
		int batchSize = 3;
		List<File> inputSentenceFiles = CollectionsUtil.createList(sentenceFile);
		Set<String> idsToExclude = new HashSet<String>();
		brat.createBratFiles(blAssociation, batchId, batchSize, inputSentenceFiles, previousSentenceIdsFile,
				idsToExclude);

		assertEquals("there should be 2 files generated", 2, outputDir.listFiles().length);

		String subBatchIndex = "aaa";
		File annFile = BratAssertionAnnotationFileCreator.getAnnFile(outputDir, blAssociation, batchId, subBatchIndex);
		File txtFile = BratAssertionAnnotationFileCreator.getTxtFile(outputDir, blAssociation, batchId, subBatchIndex);
//		File idFile = BratAssertionAnnotationFileCreator.getIdFile(outputDir, blAssociation, batchId, subBatchIndex);

		assertTrue(annFile.exists());
		assertTrue(txtFile.exists());
//		assertTrue(idFile.exists());

		List<String> txtLines = FileReaderUtil.loadLinesFromFile(txtFile, encoding);
		assertEquals("there should be 4 lines in the .txt file b/c batch size is 3 + 1 line for the DONE marker", 4,
				txtLines.size());

//		List<String> idLines = FileReaderUtil.loadLinesFromFile(idFile, encoding);
//		for (String line : idLines) {
//			System.out.println(line);
//		}
//		assertEquals("there should be 3 lines in the .id file b/c batch size is 3", 3, idLines.size());

	}

	/**
	 * batchsize now 4 to include all sentences
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBratFileGeneration2() throws IOException {
		BiolinkAssociation blAssociation = BiolinkAssociation.BL_GENE_TO_DISEASE;
		CharacterEncoding encoding = CharacterEncoding.UTF_8;

		File previousSentenceIdsFile = folder.newFile();
		File sentenceFile = folder.newFile();
		FileWriterUtil.printLines(sampleSentenceLines, sentenceFile, encoding);

		File outputDir = folder.newFolder();
		BratAssertionAnnotationFileCreator brat = new BratAssertionAnnotationFileCreator(outputDir);
		String batchId = "22";
		int batchSize = 4;
		List<File> inputSentenceFiles = CollectionsUtil.createList(sentenceFile);
		Set<String> idsToExclude = new HashSet<String>();
		brat.createBratFiles(blAssociation, batchId, batchSize, inputSentenceFiles, previousSentenceIdsFile,
				idsToExclude);

		assertEquals("there should be 2 files generated", 2, outputDir.listFiles().length);

		String subBatchIndex = "aaa";
		File annFile = BratAssertionAnnotationFileCreator.getAnnFile(outputDir, blAssociation, batchId, subBatchIndex);
		File txtFile = BratAssertionAnnotationFileCreator.getTxtFile(outputDir, blAssociation, batchId, subBatchIndex);
//		File idFile = BratAssertionAnnotationFileCreator.getIdFile(outputDir, blAssociation, batchId, subBatchIndex);

		assertTrue(annFile.exists());
		assertTrue(txtFile.exists());
//		assertTrue(idFile.exists());

		List<String> txtLines = FileReaderUtil.loadLinesFromFile(txtFile, encoding);
		assertEquals("there should be 5 lines in the .txt file b/c batch size is 4 + 1 for the DONE marker", 5,
				txtLines.size());

//		List<String> idLines = FileReaderUtil.loadLinesFromFile(idFile, encoding);
//		for (String line : idLines) {
//			System.out.println(line);
//		}
//		assertEquals("there should be 4 lines in the .id file b/c batch size is 4", 4, idLines.size());

		List<String> annLines = FileReaderUtil.loadLinesFromFile(annFile, encoding);
		assertEquals(
				"there should be 9 lines in the .ann file b/c there are 10 annotations, but one is protein	PR:000000001 which is excluded b/c it's too general to be useful.",
				9, annLines.size());

	}

	/**
	 * Testing with the multi-line sentence in the middle
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testSentenceIterator() throws FileNotFoundException, IOException {

		StringBuilder sb = new StringBuilder();
		sb.append(sampleSentenceLines.get(0) + "\n");
		sb.append(sampleSentenceLines.get(1) + "\n");
		sb.append(sampleSentenceLines.get(2) + "\n");
		sb.append(sampleSentenceLines.get(3) + "\n");
		sb.append(sampleSentenceLines.get(4) + "\n");
		sb.append(sampleSentenceLines.get(5) + "\n");

		InputStream stream = new ByteArrayInputStream(sb.toString().getBytes());
		SentenceIterator sentIter = new SentenceIterator(stream);

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(3, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertFalse(sentIter.hasNext());

	}

	/**
	 * Testing with the multi-line sentence last
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testSentenceIterator2() throws FileNotFoundException, IOException {

		StringBuilder sb = new StringBuilder();
		sb.append(sampleSentenceLines.get(0) + "\n");
		sb.append(sampleSentenceLines.get(1) + "\n");
		sb.append(sampleSentenceLines.get(5) + "\n");
		sb.append(sampleSentenceLines.get(2) + "\n");
		sb.append(sampleSentenceLines.get(3) + "\n");
		sb.append(sampleSentenceLines.get(4) + "\n");

		InputStream stream = new ByteArrayInputStream(sb.toString().getBytes());
		SentenceIterator sentIter = new SentenceIterator(stream);

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(3, sentIter.next().size());

		assertFalse(sentIter.hasNext());

	}

	/**
	 * Testing with the multi-line sentence first
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testSentenceIterator3() throws FileNotFoundException, IOException {

		StringBuilder sb = new StringBuilder();
		sb.append(sampleSentenceLines.get(2) + "\n");
		sb.append(sampleSentenceLines.get(3) + "\n");
		sb.append(sampleSentenceLines.get(4) + "\n");
		sb.append(sampleSentenceLines.get(0) + "\n");
		sb.append(sampleSentenceLines.get(1) + "\n");
		sb.append(sampleSentenceLines.get(5) + "\n");

		InputStream stream = new ByteArrayInputStream(sb.toString().getBytes());
		SentenceIterator sentIter = new SentenceIterator(stream);

		assertTrue(sentIter.hasNext());
		assertEquals(3, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertFalse(sentIter.hasNext());

	}

	/**
	 * Testing with the multi-line sentence interspersed with other sentences
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testSentenceIterator4() throws FileNotFoundException, IOException {

		StringBuilder sb = new StringBuilder();
		sb.append(sampleSentenceLines.get(2) + "\n");
		sb.append(sampleSentenceLines.get(0) + "\n");
		sb.append(sampleSentenceLines.get(4) + "\n");
		sb.append(sampleSentenceLines.get(1) + "\n");
		sb.append(sampleSentenceLines.get(3) + "\n");
		sb.append(sampleSentenceLines.get(5) + "\n");

		InputStream stream = new ByteArrayInputStream(sb.toString().getBytes());
		SentenceIterator sentIter = new SentenceIterator(stream);

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(3, sentIter.next().size());

		assertTrue(sentIter.hasNext());
		assertEquals(1, sentIter.next().size());

		assertFalse(sentIter.hasNext());

	}

}
