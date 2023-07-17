package edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIFileParser.Theme;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIIFileParser.Sentence;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.string.RegExPatterns;

public class BertTrainingFileCreator {

	public static void main(String[] args) {

		if (true) {
			// gene-gene
			File partIFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/gene-gene/part-i-gene-gene-path-theme-distributions.txt.gz");

			File partIIFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/gene-gene/part-ii-dependency-paths-gene-gene-sorted-with-themes.txt.gz");

			File bertTrainingFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/gene-gene/bert-gene-gene.training.tsv");

			try {
				Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
						.getDependencyPathToThemesMap(partIFile);
				Map<Theme, Set<Sentence>> themeToSentencesMap = PerchaAltmanPartIIFileParser
						.getThemeToSentenceMap(partIIFile, dependencyPathToThemesMap);

				// we exclude increase_expression for now as that will need to be a
				// separate classifier based on the sentences that are classified as
				// affects_expression
				Set<Theme> themesToInclude = CollectionsUtil.createSet(Theme.B_BINDING, Theme.W_ENHANCES_RESPONSE,
						Theme.Vplus_ACTIVATES, Theme.E_AFFECTS_EXPRESSION, Theme.I_SIGNALING_PATHWAY,
						Theme.Rg_REGULATION

//						Theme.H_SAME_PROTEIN_OR_COMPLEX, Theme.Q_PRODUCTION_BY_CELL_POPULATION
				);

				createBertTrainingFile(bertTrainingFile, themeToSentencesMap, themesToInclude);

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		if (false) {
			// chemical-gene
			File partIFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/chemical-gene/part-i-chemical-gene-path-theme-distributions.txt.gz");

			File partIIFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/chemical-gene/part-ii-dependency-paths-chemical-gene-sorted-with-themes.txt.gz");

			File bertTrainingFile = new File(
					"/Users/bill/projects/ncats-translator/relations/bicluster/chemical-gene/adding-negative-examples/bert-chemical-gene.training.tsv");

			try {
				Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
						.getDependencyPathToThemesMap(partIFile);
				Map<Theme, Set<Sentence>> themeToSentencesMap = PerchaAltmanPartIIFileParser
						.getThemeToSentenceMap(partIIFile, dependencyPathToThemesMap);

				// we exclude increase/decrease_expression for now as that will need to be a
				// separate classifier based on the sentences that are classified as
				// affects_expression
				Set<Theme> themesToInclude = CollectionsUtil.createSet(Theme.Aminus_ANTAGONISM, Theme.Aplus_AGONISM,
						Theme.B_BINDING, Theme.E_AFFECTS_EXPRESSION, Theme.N_INHIBITS
				// Excluding gene-to-chemical assertions
				// , Theme.K_METABOLISM, Theme.O_TRANSPORTS, Theme.Z_ENZYME_ACTIVITY
				);

				createBertTrainingFile(bertTrainingFile, themeToSentencesMap, themesToInclude);

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	private static void createBertTrainingFile(File bertTrainingFile, Map<Theme, Set<Sentence>> themeToSentencesMap,
			Set<Theme> themesToInclude) throws IOException {
		Set<String> alreadyOutputSentenceId = new HashSet<String>();

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(bertTrainingFile)) {
			for (Theme t : themesToInclude) {
				for (Sentence s : themeToSentencesMap.get(t)) {
					String sentence = getOriginalSentenceWithPlaceholders(s);
					String sentenceId = DigestUtils.sha1Hex(sentence.getBytes());
					if (!alreadyOutputSentenceId.contains(sentenceId)) {
						alreadyOutputSentenceId.add(sentenceId);
						writer.write(String.format("%s\t%s\t%s\n", sentenceId, sentence, t.name().toLowerCase()));
					}
				}
			}
		}

	}

	/**
	 * Detokenizes the sentence and replaces the entities with placeholders
	 * 
	 * @param tokenizedSentence
	 * @return
	 */
	private static String getOriginalSentenceWithPlaceholders(Sentence sentence) {
		String originalSentence = deTokenize(sentence);
		String origSentWithPlaceholders = replaceEntitiesWithPlaceholders(originalSentence, sentence);
		return origSentWithPlaceholders;
	}

	private static String replaceEntitiesWithPlaceholders(String originalSentence, Sentence sentence) {
//		System.out.println("  TOK: " + sentence.getTokenizedSentence());
//		System.out.println("DETOK: " + originalSentence);
//
//		System.out.println(sentence.toString());
//		throw new IllegalArgumentException();

		String entity1Text = RegExPatterns.escapeCharacterForRegEx(sentence.getEntity1NameFormatted());
		String entity1Placeholder = RegExPatterns.escapeCharacterForRegEx(getPlaceholder(sentence.getEntity1Type()));

		String entity2Text = RegExPatterns.escapeCharacterForRegEx(sentence.getEntity2NameFormatted());
		String entity2Placeholder = RegExPatterns.escapeCharacterForRegEx(getPlaceholder(sentence.getEntity2Type()));

		// we could be more careful in the placeholder replacement here, but since the
		// spans are relative to the beginning of the abstract, not the beginning of the
		// sentence, they are somewhat difficult to use. So, we are simply replacing the
		// entities with their first instance in the sentence. It's possible that an
		// entity could be listed twice (or more) in a sentence. In those cases we may
		// be replacing the wrong string with the placeholder. If this works and we end
		// up using this method with our own data then we can make sure that the
		// placeholders are put in the proper position always.

//		System.out.println("sentence: " + originalSentence);
//		System.out.println("entity1 text: " + entity1Text);
//		System.out.println("entity1 placeholder: " + entity1Placeholder);

		originalSentence = originalSentence.replaceFirst(entity1Text, entity1Placeholder);
		originalSentence = originalSentence.replaceFirst(entity2Text, entity2Placeholder);

		return originalSentence;
	}

	private static String getPlaceholder(String entityType) {
		return String.format("@%s$", entityType.toUpperCase());
	}

	private static String deTokenize(Sentence sentence) {
		String deTokSent = sentence.getTokenizedSentence();
		deTokSent = deTokSent.replaceAll("-LRB- ", "(");
		deTokSent = deTokSent.replaceAll("-LSB- ", "[");
		deTokSent = deTokSent.replaceAll(" -RRB-", ")");
		deTokSent = deTokSent.replaceAll(" -RSB-", "]");
		deTokSent = deTokSent.replaceAll(" ,", ",");
		deTokSent = deTokSent.replaceAll(" \\.", ".");
		return deTokSent;
	}

}
