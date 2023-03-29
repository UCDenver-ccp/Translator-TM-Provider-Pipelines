package edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIFileParser.Theme;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIIFileParser.Sentence;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;

public class PerchaAltmanPartIIFileParserTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private static List<String> getSamplePartIFileLinesChemicalToGene() {
		return Arrays.asList(
				"path	A+	A+.ind	A-	A-.ind	B	B.ind	E+	E+.ind	E-	E-.ind	E	E.ind	N	N.ind	O	O.ind	K	K.ind	Z	Z.ind\n"
						+ "kinases|compound|start_entity participate|nsubj|kinases participate|nmod|end_entity	222.000000	1	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	2.000000	0	0.000000	0	0.000000	0\n"
						+ "treatment|nmod|start_entity caused|nsubj|treatment caused|nmod|activity activity|amod|end_entity	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	4.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0\n"
						+ "synthesis|nmod|start_entity analogues|nsubj|synthesis analogues|nmod|modulators modulators|nmod|activity activity|compound|end_entity	0.000000	0	0.000000	0	0.000000	0	555.000000	1	1.000000	0	777.000000	1	1.000000	0	0.000000	0	0.000000	0	0.000000	0\n"
						+ "phosphate|amod|start_entity hydrolysis|nmod|phosphate catalyzes|dobj|hydrolysis catalyzes|nsubj|reticulum reticulum|amod|end_entity	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	1.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0\n");
	}

	private File getSamplePartIFileChemicalToGene() throws IOException {
		File file = folder.newFile();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(file)),
				CharacterEncoding.UTF_8)) {
			for (String line : getSamplePartIFileLinesChemicalToGene()) {
				writer.write(line);
			}
		}
		return file;
	}

	private static List<String> getSamplePartIIFileLinesChemicalToGene() {
		return Arrays.asList(
				"25640386	11	-	1624,1625	hormone_receptor	1598,1614	-	hormone receptor	null	3164	Chemical	Gene	kinases|compound|START_ENTITY participate|nsubj|kinases participate|nmod|END_ENTITY	Similarly 18F-FDG-PET/CT had higher sensitivity and specificity in hormone_receptor -LRB- + -RRB- and -LRB- - -RRB- groups .\n"
						+ "28560459	1	+	244,245	Sig-1R	227,233	+	Sig-1R	null	10280	Chemical	Gene	kinases|compound|START_ENTITY participate|nsubj|kinases participate|nmod|END_ENTITY	The purpose of the present study was to investigate the protective effect of the -1 receptor -LRB- Sig-1R -RRB- agonist -LRB- + -RRB- - pentazocin -LRB- PTZ -RRB- on pressure-induced apoptosis and death of human trabecular meshwork cells -LRB- hTMCs -RRB- .\n"
						+ "637620	2	0,0-diethyl_0-4-nitrophenyl_phosphate	634,671	cholinesterase	452,466	0,0-diethyl 0-4-nitrophenyl phosphate	cholinesterase	null	65036(Tax:10116)	Chemical	Gene	synthesis|nmod|START_ENTITY analogues|nsubj|synthesis analogues|nmod|modulators modulators|nmod|activity activity|compound|END_ENTITY	It decreased by 51 % cholinesterase inhibition in the brain caused by i.p. injection of 2 mg of parathion/kg body weight but not that of an equitoxic dose -LRB- 0.5 mg/kg -RRB- of its active metabolite , paraoxon -LRB- 0,0-diethyl_0-4-nitrophenyl_phosphate -RRB- .\n"
						+ "20812894	8	01CM53122	1498,1507	env	1515,1518	01CM53122	env	null	30816	Chemical	Gene	treatment|nmod|START_ENTITY caused|nsubj|treatment caused|nmod|activity activity|amod|END_ENTITY	Further analysis of the 01CM53122 genome showed that this virus represents a diverse set of mosaic genomes from CRF22_01A1 , including a 446-nt segment of 01CM53122 in the env region , but unlike other CRF22 strains , clustered with CRF01_AE rather than the A1 sequence , suggesting that the 01CM53122 strain is a recombinant of CRF22_01A1 and CRF01_AE .\n"
						+ "14564379	6	0-5-10-15_cmH2O	689,704	CPAP	676,680	0-5-10-15 cmH2O	CPAP	null	55835	Chemical	Gene	phosphate|amod|START_ENTITY hydrolysis|nmod|phosphate catalyzes|dobj|hydrolysis catalyzes|nsubj|reticulum reticulum|amod|END_ENTITY	Three gas flow rates -LRB- 20-30-40 l/min and 30-45-60 l/min , respectively , for CPAPM and CPAPH -RRB- and four CPAP levels -LRB- 0-5-10-15_cmH2O -RRB- were employed in a randomized sequence .\n"

		);
	}

	private File getSamplePartIIFileChemicalToGene() throws IOException {
		File file = folder.newFile();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(file)),
				CharacterEncoding.UTF_8)) {
			for (String line : getSamplePartIIFileLinesChemicalToGene()) {
				writer.write(line);
			}
		}
		return file;
	}

	@Test
	public void testGetThemeToSentenceMapChemicalToGene() throws FileNotFoundException, IOException {

		Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
				.getDependencyPathToThemesMap(getSamplePartIFileChemicalToGene());
		Map<Theme, Set<Sentence>> themeToSentenceMap = PerchaAltmanPartIIFileParser
				.getThemeToSentenceMap(getSamplePartIIFileChemicalToGene(), dependencyPathToThemesMap);

		Map<Theme, Set<Sentence>> expectedMap = new HashMap<Theme, Set<Sentence>>();

		// 25640386 11 - 1624,1625 hormone_receptor 1598,1614 - hormone receptor null
		// 3164 Chemical Gene kinases|compound|START_ENTITY participate|nsubj|kinases
		// participate|nmod|END_ENTITY Similarly 18F-FDG-PET/CT had higher sensitivity
		// and specificity in hormone_receptor -LRB- + -RRB- and -LRB- - -RRB- groups .
		int pubmedId1 = 25640386;
		int sentenceNum1 = 11;
		String entity1NameFormatted1 = "-";
		String entity1NameRaw1 = "-";
		String entity1Ids1 = "null";
		String entity1Type1 = "Chemical";
		Span entity1Span1 = new Span(1624, 1625);
		String entity2NameFormatted1 = "hormone_receptor";
		String entity2NameRaw1 = "hormone receptor";
		String entity2Ids1 = "3164";
		String entity2Type1 = "Gene";
		Span entity2Span1 = new Span(1598, 1614);
		String dependencyPath1 = "kinases|compound|start_entity participate|nsubj|kinases participate|nmod|end_entity";
		String tokenizedSentence1 = "Similarly 18F-FDG-PET/CT had higher sensitivity and specificity in hormone_receptor -LRB- + -RRB- and -LRB- - -RRB- groups .";
		Sentence sent1 = new Sentence(pubmedId1, sentenceNum1, entity1NameFormatted1, entity1NameRaw1, entity1Ids1,
				entity1Type1, entity1Span1, entity2NameFormatted1, entity2NameRaw1, entity2Ids1, entity2Type1,
				entity2Span1, dependencyPath1, tokenizedSentence1);

		// 28560459 1 + 244,245 Sig-1R 227,233 + Sig-1R null 10280 Chemical Gene
		// kinases|compound|START_ENTITY participate|nsubj|kinases
		// participate|nmod|END_ENTITY The purpose of the present study was to
		// investigate the protective effect of the -1 receptor -LRB- Sig-1R -RRB-
		// agonist -LRB- + -RRB- - pentazocin -LRB- PTZ -RRB- on pressure-induced
		// apoptosis and death of human trabecular meshwork cells -LRB- hTMCs -RRB- .
		int pubmedId2 = 28560459;
		int sentenceNum2 = 1;
		String entity1NameFormatted2 = "+";
		String entity1NameRaw2 = "+";
		String entity1Ids2 = "null";
		String entity1Type2 = "Chemical";
		Span entity1Span2 = new Span(244, 245);
		String entity2NameFormatted2 = "Sig-1R";
		String entity2NameRaw2 = "Sig-1R";
		String entity2Ids2 = "10280";
		String entity2Type2 = "Gene";
		Span entity2Span2 = new Span(227, 233);
		String dependencyPath2 = "kinases|compound|start_entity participate|nsubj|kinases participate|nmod|end_entity";
		String tokenizedSentence2 = "The purpose of the present study was to investigate the protective effect of the -1 receptor -LRB- Sig-1R -RRB- agonist -LRB- + -RRB- - pentazocin -LRB- PTZ -RRB- on pressure-induced apoptosis and death of human trabecular meshwork cells -LRB- hTMCs -RRB- .";
		Sentence sent2 = new Sentence(pubmedId2, sentenceNum2, entity1NameFormatted2, entity1NameRaw2, entity1Ids2,
				entity1Type2, entity1Span2, entity2NameFormatted2, entity2NameRaw2, entity2Ids2, entity2Type2,
				entity2Span2, dependencyPath2, tokenizedSentence2);

		// 637620 2 0,0-diethyl_0-4-nitrophenyl_phosphate 634,671 cholinesterase 452,466
		// 0,0-diethyl 0-4-nitrophenyl phosphate cholinesterase null 65036(Tax:10116)
		// Chemical Gene synthesis|nmod|START_ENTITY analogues|nsubj|synthesis
		// analogues|nmod|modulators modulators|nmod|activity
		// activity|compound|END_ENTITY It decreased by 51 % cholinesterase inhibition
		// in the brain caused by i.p. injection of 2 mg of parathion/kg body weight but
		// not that of an equitoxic dose -LRB- 0.5 mg/kg -RRB- of its active metabolite
		// , paraoxon -LRB- 0,0-diethyl_0-4-nitrophenyl_phosphate -RRB- .
		int pubmedId3 = 637620;
		int sentenceNum3 = 2;
		String entity1NameFormatted3 = "0,0-diethyl_0-4-nitrophenyl_phosphate";
		String entity1NameRaw3 = "0,0-diethyl 0-4-nitrophenyl phosphate";
		String entity1Ids3 = "null";
		String entity1Type3 = "Chemical";
		Span entity1Span3 = new Span(634, 671);
		String entity2NameFormatted3 = "cholinesterase";
		String entity2NameRaw3 = "cholinesterase";
		String entity2Ids3 = "65036(Tax:10116)";
		String entity2Type3 = "Gene";
		Span entity2Span3 = new Span(452, 466);
		String dependencyPath3 = "synthesis|nmod|start_entity analogues|nsubj|synthesis analogues|nmod|modulators modulators|nmod|activity activity|compound|end_entity";
		String tokenizedSentence3 = "It decreased by 51 % cholinesterase inhibition in the brain caused by i.p. injection of 2 mg of parathion/kg body weight but not that of an equitoxic dose -LRB- 0.5 mg/kg -RRB- of its active metabolite , paraoxon -LRB- 0,0-diethyl_0-4-nitrophenyl_phosphate -RRB- .";
		Sentence sent3 = new Sentence(pubmedId3, sentenceNum3, entity1NameFormatted3, entity1NameRaw3, entity1Ids3,
				entity1Type3, entity1Span3, entity2NameFormatted3, entity2NameRaw3, entity2Ids3, entity2Type3,
				entity2Span3, dependencyPath3, tokenizedSentence3);

		expectedMap.put(Theme.Aplus_AGONISM, CollectionsUtil.createSet(sent1, sent2));
		expectedMap.put(Theme.Eplus_INCREASES_EXPRESSION, CollectionsUtil.createSet(sent3));
		expectedMap.put(Theme.E_AFFECTS_EXPRESSION, CollectionsUtil.createSet(sent3));

		assertEquals(expectedMap, themeToSentenceMap);

	}

}
