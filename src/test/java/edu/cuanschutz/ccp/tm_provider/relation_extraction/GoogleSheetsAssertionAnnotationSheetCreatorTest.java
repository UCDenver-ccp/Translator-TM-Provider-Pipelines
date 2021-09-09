package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

public class GoogleSheetsAssertionAnnotationSheetCreatorTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testCountSentences() throws IOException {
		File sentenceFile = folder.newFile();

		List<String> lines = Arrays.asList(getSentenceFileLine("sentence 1"), getSentenceFileLine("sentence 1"),
				getSentenceFileLine("sentence 1"), getSentenceFileLine("sentence 2"), getSentenceFileLine("sentence 3"),
				getSentenceFileLine("sentence 4"), getSentenceFileLine("sentence 4"), getSentenceFileLine("sentence 4"),
				getSentenceFileLine("sentence 4"), getSentenceFileLine("sentence 5"));

		FileWriterUtil.printLines(lines, sentenceFile, CharacterEncoding.UTF_8);

		int sentenceCount = GoogleSheetsAssertionAnnotationSheetCreator.countSentences(sentenceFile);

		assertEquals("expected 5 unique sentences", 5, sentenceCount);

	}

	private String getSentenceFileLine(String sentenceText) {
		return CollectionsUtil.createDelimitedString(
				Arrays.asList("", "", "", "", "", "", "", "", "", "", sentenceText, "", "", ""), "\t");
	}

}
