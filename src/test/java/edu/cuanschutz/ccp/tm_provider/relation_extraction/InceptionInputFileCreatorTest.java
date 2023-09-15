package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class InceptionInputFileCreatorTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testLoadAlreadyAnnotatedSentenceIds() throws IOException {
		File outputFile1 = folder.newFile();
		// write 5 sentences to the file
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile1)) {
			writer.write(getSampleSentence(1));
			writer.write(getSampleSentence(2));
			writer.write(getSampleSentence(3));
			writer.write(getSampleSentence(4));
		}
		File outputFile2 = folder.newFile();
		// write 5 sentences to the file
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile2)) {
			writer.write(getSampleSentence(5));
			writer.write(getSampleSentence(6));
			writer.write(getSampleSentence(7));
			writer.write(getSampleSentence(8));
		}

		Set<String> alreadyAnnotatedSentenceIds = InceptionInputFileCreator
				.loadAlreadyAnnotatedSentenceIds(CollectionsUtil.createList(outputFile1, outputFile2));

		Set<String> expectedHashes = new HashSet<String>();

		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 1."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 2."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 3."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 4."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 5."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 6."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 7."));
		expectedHashes.add(InceptionInputFileCreator
				.computeHash("#Text=The role of heat shock proteins and co-chaperones in heart failure 8."));

		assertEquals(expectedHashes.size(), alreadyAnnotatedSentenceIds.size());
		assertEquals(expectedHashes, alreadyAnnotatedSentenceIds);

	}

	@Test
	public void testCountSentences() throws FileNotFoundException, IOException {
		String sentence = getSampleSentence(1);

		File outputFile = folder.newFile();
		// write 5 sentences to the file
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			writer.write(sentence);
			writer.write(sentence);
			writer.write(sentence);
			writer.write(sentence);
			writer.write(sentence);
		}

		int sentenceCount = InceptionInputFileCreator.countSentences(outputFile);
		assertEquals("should have 5 sentences", 5, sentenceCount);
	}

	private String getSampleSentence(int key) {
		return getSampleSentence(key, 1, 0, 2);
	}

	private String getSampleSentence(int key, int sentenceNum, int offset, int multiTokenIndex) {
		// @formatter:off
		String sentence = "\n#Text=The role of heat shock proteins and co-chaperones in heart failure "+key+".\n" + 
				sentenceNum + "-1\t"+ (0+ offset)  + "-" + (3+offset) + "\tThe\t_\n" + 
				sentenceNum + "-2\t"+ (4+ offset)  + "-" + (8+offset) + "\trole\t_\n" + 
				sentenceNum + "-3\t"+ (9+ offset)  + "-" + (11+offset) + "\tof\t_\n" + 
				sentenceNum + "-4\t"+ (12+ offset)  + "-" + (16+offset) + "\theat\t_\n" + 
				sentenceNum + "-5\t"+ (17+ offset)  + "-" + (22+offset) + "\tshock\t_\n" + 
				sentenceNum + "-6\t"+ (23+ offset)  + "-" + (31+offset) + "\tproteins\tCHEMICAL\n" + 
				sentenceNum + "-7\t"+ (32+ offset)  + "-" + (35+offset) + "\tand\t_\n" + 
				sentenceNum + "-8\t"+ (36+ offset)  + "-" + (49+offset) + "\tco-chaperones\t_\n" + 
				sentenceNum + "-9\t"+ (50+ offset)  + "-" + (52+offset) + "\tin\t_\n" + 
				sentenceNum + "-10\t"+ (53+ offset)  + "-" + (58+offset) + "\theart\tDISEASE["+multiTokenIndex+"]\n" + 
				sentenceNum + "-11\t"+ (59+ offset)  + "-" + (66+offset) + "\tfailure\tDISEASE["+multiTokenIndex+"]\n" + 
				sentenceNum + "-12\t"+ (66+ offset)  + "-" + (67+offset) + "\t.\t_\n";
		// @formatter:on
		return sentence;
	}

	private List<String> getSampleSentenceData(int key) {
		String sampleSentence = getSampleSentence(key);
		List<String> list = new ArrayList<String>(Arrays.asList(sampleSentence.split("\\n")));
		list.remove(0);
		return list;
	}

	@Test
	public void testGetRandomIndexes() {
		Set<Integer> randomIndexes = InceptionInputFileCreator.getRandomIndexes(123456, 100);
		assertEquals("should have batchSize + 100 random indexes.", 200, randomIndexes.size());
	}

	@Test
	public void testGetNextSentence() throws IOException {
		File outputFile = folder.newFile();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			writer.write(getSampleSentence(1));
			writer.write(getSampleSentence(2));
			writer.write(getSampleSentence(3));
			writer.write(getSampleSentence(4));
		}

		StreamLineIterator lineIter = new StreamLineIterator(outputFile, CharacterEncoding.UTF_8);

		List<String> sentenceData = InceptionInputFileCreator.getNextSentence(lineIter);
		assertTrue(sentenceData.get(0)
				.contains("#Text=The role of heat shock proteins and co-chaperones in heart failure 1."));
		sentenceData = InceptionInputFileCreator.getNextSentence(lineIter);
		assertTrue(sentenceData.get(0)
				.contains("#Text=The role of heat shock proteins and co-chaperones in heart failure 2."));
		sentenceData = InceptionInputFileCreator.getNextSentence(lineIter);
		assertTrue(sentenceData.get(0)
				.contains("#Text=The role of heat shock proteins and co-chaperones in heart failure 3."));
		sentenceData = InceptionInputFileCreator.getNextSentence(lineIter);
		assertTrue(sentenceData.get(0)
				.contains("#Text=The role of heat shock proteins and co-chaperones in heart failure 4."));
		sentenceData = InceptionInputFileCreator.getNextSentence(lineIter);
		assertTrue(sentenceData.isEmpty());

	}

	@Test
	public void testUpdateSentenceData() {
		InceptionInputFileCreator iifc = new InceptionInputFileCreator();
		iifc.setGlobalCharacterOffset(1834);
		iifc.setGlobalMultiTokenEntityIndex(123);

		String updatedSentenceData = iifc.updateSentenceData(1234, getSampleSentenceData(1));

		String expectedSentenceData = getSampleSentence(1, 1234, 1834, 124).substring(1);

		assertEquals(expectedSentenceData, updatedSentenceData);

	}

	@Test
	public void testUpdateEntityStr() {
		InceptionInputFileCreator iifc = new InceptionInputFileCreator();
		iifc.setGlobalMultiTokenEntityIndex(103);
		Map<String, String> originalMultiTokenEntityIndexToUpdated = new HashMap<String, String>();
		originalMultiTokenEntityIndexToUpdated.put("[4]", "[100]");

		String entityStr = "DISEASE[4]|CHEMICAL[7]";
		String updateEntityStr = iifc.updateEntityStr(entityStr, originalMultiTokenEntityIndexToUpdated);

		assertEquals("DISEASE[100]|CHEMICAL[104]", updateEntityStr);
		assertEquals(104, iifc.getGlobalMultiTokenEntityIndex());
	}

	@Test
	public void testGetTokenLine() {
		String expectedTokenLine = "32-3\t150-162\tfacilitating\t_\n";
		String tokenLine = InceptionInputFileCreator.getTokenLine(32, 3, 150l, 162l, "facilitating", "_");

		assertEquals(expectedTokenLine, tokenLine);

	}

}
