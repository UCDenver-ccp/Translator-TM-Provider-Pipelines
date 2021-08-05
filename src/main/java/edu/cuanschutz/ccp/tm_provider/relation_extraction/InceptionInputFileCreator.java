package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.google.api.client.util.Base64;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Data;

/**
 * Takes as input a file containing sentences pulled using the
 * WebAnnoSentenceExtractionPipeline. In order to load sentences into Inception
 * for annotation, a number of changes are necessary.
 * 
 * 1) a header needs to be added <br>
 * 2) a subset of the sentences should be randomly selected<br>
 * 3) the character offsets need to be adjusted such that they are consecutive
 * throughout the entire file<br>
 * 4) the indexes for multi-token entities need to be adjusted so that they are
 * unique<br>
 * 5) the sentence numbers need to be incremented
 *
 */
@Data
public class InceptionInputFileCreator {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;
	private long globalCharacterOffset = 0;
	private long globalMultiTokenEntityIndex = 0;

	/**
	 * @param inputSentenceFile
	 * @param outputFile
	 * @param previousSubsetFiles - to ensure that the new sentence file contains
	 *                            unique sentences
	 */
	public void createNewSubset(int batchSize, File inputSentenceFile, File outputFile,
			Collection<File> previousSubsetFiles) throws IOException {

		Set<String> alreadyAnnotated = loadAlreadyAnnotatedSentenceIds(previousSubsetFiles);

		int maxSentenceCount = countSentences(inputSentenceFile);

		Set<Integer> indexesForNewBatch = getRandomIndexes(maxSentenceCount, batchSize);

		int sentenceIndex = 1;
		int extractedSentenceCount = 1;
		InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
				? new GZIPInputStream(new FileInputStream(inputSentenceFile))
				: new FileInputStream(inputSentenceFile);
		StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null);
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			/* write the file header */
			writer.write("#FORMAT=WebAnno TSV 3.3\n" + "#T_SP=custom.Span|label\n" + "\n" + "\n");
			while (extractedSentenceCount < batchSize) {
				List<String> sentenceData = getNextSentence(lineIter);
				if (sentenceData == null) {
					break;
				}
				if (indexesForNewBatch.contains(sentenceIndex++)) {
					String sentenceText = sentenceData.get(0);
					String hash = computeHash(sentenceText);
					if (!alreadyAnnotated.contains(hash)) {
						alreadyAnnotated.add(hash);
						String updatedSentenceData = updateSentenceData(extractedSentenceCount++, sentenceData);
						writer.write(updatedSentenceData);
						// avoid line break on the final sentence
						if (extractedSentenceCount < batchSize) {
							writer.write("\n");
						}
					}
				}
			}
		} finally {
			lineIter.close();
		}

	}

	/**
	 * Cycles through the input files, looks for the sentence lines (lines that
	 * begin with "#Text=", and returns the set of hashes for all sentences
	 * observed.
	 * 
	 * @param previousSubsetFiles
	 * @return
	 * @throws IOException
	 */
	protected static Set<String> loadAlreadyAnnotatedSentenceIds(Collection<File> previousSubsetFiles)
			throws IOException {
		Set<String> hashes = new HashSet<String>();
		if (previousSubsetFiles != null) {
			for (File previousSubsetFile : previousSubsetFiles) {
				for (StreamLineIterator lineIter = new StreamLineIterator(previousSubsetFile, UTF8); lineIter
						.hasNext();) {
					String line = lineIter.next().getText();
					if (line.startsWith("#Text=")) {
						hashes.add(computeHash(line));
					}
				}
			}
		}
		return hashes;
	}

	protected static String computeHash(String line) {
		return Base64.encodeBase64String(line.getBytes());
	}

	/**
	 * @param inputSentenceFile
	 * @return the number of sentences in the specified file (as determined by
	 *         counting lines that begin with #Text=)
	 */
	protected static int countSentences(File inputSentenceFile) throws IOException {
		int sentenceCount = 0;
		InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
				? new GZIPInputStream(new FileInputStream(inputSentenceFile))
				: new FileInputStream(inputSentenceFile);
		for (StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null); lineIter.hasNext();) {
			String line = lineIter.next().getText();
			if (line.startsWith("#Text=")) {
				sentenceCount++;
			}
		}
		return sentenceCount;
	}

	protected static Set<Integer> getRandomIndexes(int maxSentenceCount, int batchSize) {
		Set<Integer> randomIndexes = new HashSet<Integer>();

		// add 100 extra just in case there are collisions with previous extracted
		// sentences
		Random rand = new Random();
		while (randomIndexes.size() < batchSize + 100) {
			randomIndexes.add(rand.nextInt(maxSentenceCount) + 1);
		}

		return randomIndexes;
	}

	protected static List<String> getNextSentence(StreamLineIterator lineIter) {
		List<String> lines = new ArrayList<String>();

		// advance iterator until the line starts with #Text=
		while (lineIter.hasNext()) {
			String line = lineIter.next().getText();
			if (line.startsWith("#Text=")) {
				lines.add(line);
				break;
			}
		}

		// now grab all lines until a blank line
		while (lineIter.hasNext()) {
			String line = lineIter.next().getText();
			if (!line.trim().isEmpty()) {
				lines.add(line);
			} else {
				break;
			}
		}

		return lines;
	}

	protected String updateSentenceData(int sentenceNum, List<String> sentenceData) {
		long globalCharacterOffset = getGlobalCharacterOffset();
		long globalMultTokenEntityIndex = getGlobalMultiTokenEntityIndex();
		StringBuilder builder = new StringBuilder();

		Pattern p = Pattern.compile("^\\d+-(\\d+)\\t(\\d+)-(\\d+)\t(.*?)\\t(.*?)$");
		long spanStart = -1;
		long spanEnd = -1;

		Map<String, String> originalMultiTokenEntityIndexToUpdated = new HashMap<String, String>();
		for (String line : sentenceData) {
			if (line.startsWith("#Text=")) {
				builder.append(line + "\n");
				continue;
			}
			/**
			 * otherwise the line should look like:
			 * 
			 * <pre>
			 *  1-6     22-30   emerging        _
			 * </pre>
			 */
			Matcher m = p.matcher(line);
			if (m.find()) {
				int tokenNum = Integer.parseInt(m.group(1));
				String spanStartStr = m.group(2);
				String spanEndStr = m.group(3);
				String coveredText = m.group(4);
				String entityStr = m.group(5);

				spanStart = Integer.parseInt(spanStartStr) + globalCharacterOffset;
				spanEnd = Integer.parseInt(spanEndStr) + globalCharacterOffset;

				String updatedEntityStr = updateEntityStr(entityStr, originalMultiTokenEntityIndexToUpdated);

				builder.append(getTokenLine(sentenceNum, tokenNum, spanStart, spanEnd, coveredText, updatedEntityStr));
			} else {
				throw new IllegalArgumentException("Unable to match expected token line to pattern: " + line);
			}

		}

		setGlobalCharacterOffset(spanEnd + 1);
//		setGlobalMultiTokenEntityIndex(globalMultTokenEntityIndex);
		return builder.toString();
	}

	protected String updateEntityStr(String entityStr, Map<String, String> originalMultiTokenEntityIndexToUpdated) {
		Pattern p = Pattern.compile("\\[\\d+\\]");
		Matcher m = p.matcher(entityStr);
		String updatedEntityStr = entityStr;
		while (m.find()) {
			// check to see if we have already updated this entity index, if not, assign it
			// the next index and log it in the map
			String index = m.group();
			if (!originalMultiTokenEntityIndexToUpdated.containsKey(index)) {
				incrementGlobalMultiTokenEntityIndex();
				String nextIndex = String.format("[%d]", getGlobalMultiTokenEntityIndex());
				originalMultiTokenEntityIndexToUpdated.put(index, nextIndex);
			}
			updatedEntityStr = updatedEntityStr.replace(index, originalMultiTokenEntityIndexToUpdated.get(index));
		}

		// else no need to update
		return updatedEntityStr;
	}

	private void incrementGlobalMultiTokenEntityIndex() {
		this.globalMultiTokenEntityIndex++;

	}

	protected static String getTokenLine(int sentenceNum, int tokenNum, long spanStart, long spanEnd,
			String coveredText, String updatedEntityStr) {
		return String.format("%d-%d\t%d-%d\t%s\t%s\n", sentenceNum, tokenNum, spanStart, spanEnd, coveredText,
				updatedEntityStr);
	}

}
