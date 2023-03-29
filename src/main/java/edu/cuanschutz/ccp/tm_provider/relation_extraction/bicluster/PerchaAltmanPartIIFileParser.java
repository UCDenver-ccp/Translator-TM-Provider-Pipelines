package edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIFileParser.Theme;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import lombok.Data;

public class PerchaAltmanPartIIFileParser {

	@Data
	public static class Sentence {
		private final int pubmedId;
		private final int sentenceNum;
		private final String entity1NameFormatted;
		private final String entity1NameRaw;
		private final String entity1Ids;
		private final String entity1Type;
		private final Span entity1Span;
		private final String entity2NameFormatted;
		private final String entity2NameRaw;
		private final String entity2Ids;
		private final String entity2Type;
		private final Span entity2Span;
		private final String dependencyPath;
		private final String tokenizedSentence;
	}

	public static Map<Theme, Set<Sentence>> getThemeToSentenceMap(File partIIWithThemesFile,
			Map<String, Set<Theme>> dependencyPathToThemeMap) throws FileNotFoundException, IOException {
		Map<Theme, Set<Sentence>> themeToSentenceMap = new HashMap<Theme, Set<Sentence>>();

		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(partIIWithThemesFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println("progress -- " + line.getLineNumber());
			}

			Sentence sentence = getSentence(line);

			if (dependencyPathToThemeMap.containsKey(sentence.getDependencyPath())) {
				for (Theme t : dependencyPathToThemeMap.get(sentence.getDependencyPath())) {
					CollectionsUtil.addToOne2ManyUniqueMap(t, sentence, themeToSentenceMap);
				}
			}

		}

		return themeToSentenceMap;
	}

	private static Sentence getSentence(Line line) {
		String[] cols = line.getText().split("\\t");
		int index = 0;
		int pmid = Integer.parseInt(cols[index++]);
		int sentenceNum = Integer.parseInt(cols[index++]);
		String entity1NameFormatted = cols[index++];
		Span entity1Span = getSpan(cols[index++]);
		String entity2NameFormatted = cols[index++];
		Span entity2Span = getSpan(cols[index++]);
		String entity1NameRaw = cols[index++];
		String entity2NameRaw = cols[index++];
		String entity1Ids = cols[index++];
		String entity2Ids = cols[index++];
		String entity1Type = cols[index++];
		String entity2Type = cols[index++];
		String dependencyPath = cols[index++].toLowerCase();
		String sentenceTokenized = cols[index++];

		return new Sentence(pmid, sentenceNum, entity1NameFormatted, entity1NameRaw, entity1Ids, entity1Type,
				entity1Span, entity2NameFormatted, entity2NameRaw, entity2Ids, entity2Type, entity2Span, dependencyPath,
				sentenceTokenized);

	}

	private static Span getSpan(String s) {
		String[] cols = s.split(",");
		return new Span(Integer.parseInt(cols[0]), Integer.parseInt(cols[1]));
	}

	public static void main(String[] args) {

		File partIFile = new File(
				"/Users/bill/projects/ncats-translator/relations/bicluster/part-i-chemical-gene-path-theme-distributions.txt.gz");

		File partIIFile = new File(
				"/Users/bill/projects/ncats-translator/relations/bicluster/part-ii-dependency-paths-chemical-gene-sorted-with-themes.txt.gz");

		try {
			Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
					.getDependencyPathToThemesMap(partIFile);
			Map<Theme, Set<Sentence>> themeToSentenceMap = getThemeToSentenceMap(partIIFile, dependencyPathToThemesMap);

			Map<Theme, Set<Sentence>> sortedMap = CollectionsUtil.sortMapByKeys(themeToSentenceMap,
					SortOrder.ASCENDING);
			for (Entry<Theme, Set<Sentence>> entry : sortedMap.entrySet()) {
				System.out.println(entry.getKey().name() + " -- sentence_count: " + entry.getValue().size());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
