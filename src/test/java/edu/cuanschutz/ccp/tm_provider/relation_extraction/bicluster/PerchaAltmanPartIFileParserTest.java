package edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.BufferedWriter;
import java.io.File;
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

import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIFileParser.Path;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster.PerchaAltmanPartIFileParser.Theme;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

public class PerchaAltmanPartIFileParserTest {

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

	@Test
	public void testGetFlagshipPathsChemicalToGene() throws IOException {
		List<Path> flagshipPaths = PerchaAltmanPartIFileParser.getFlagshipPaths(getSamplePartIFileChemicalToGene());

		assertFalse(flagshipPaths.isEmpty());

		Path p1 = new Path("kinases|compound|start_entity participate|nsubj|kinases participate|nmod|end_entity",
				Arrays.asList(Theme.Aplus_AGONISM), Arrays.asList(222.000000));
		Path p2 = new Path(
				"synthesis|nmod|start_entity analogues|nsubj|synthesis analogues|nmod|modulators modulators|nmod|activity activity|compound|end_entity",
				Arrays.asList(Theme.Eplus_INCREASES_EXPRESSION, Theme.E_AFFECTS_EXPRESSION),
				Arrays.asList(555.000000, 777.000000));
		List<Path> expectedFlagshipPaths = Arrays.asList(p1, p2);

		assertEquals(expectedFlagshipPaths, flagshipPaths);

	}

	@Test
	public void testGetDependencyPathToThemesMapChemicalToGene() throws IOException {
		Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
				.getDependencyPathToThemesMap(getSamplePartIFileChemicalToGene());

		assertFalse(dependencyPathToThemesMap.isEmpty());

		Map<String, Set<Theme>> expectedMap = new HashMap<String, Set<Theme>>();

		expectedMap.put("kinases|compound|start_entity participate|nsubj|kinases participate|nmod|end_entity",
				CollectionsUtil.createSet(Theme.Aplus_AGONISM));
		expectedMap.put(
				"synthesis|nmod|start_entity analogues|nsubj|synthesis analogues|nmod|modulators modulators|nmod|activity activity|compound|end_entity",
				CollectionsUtil.createSet(Theme.Eplus_INCREASES_EXPRESSION, Theme.E_AFFECTS_EXPRESSION));

		assertEquals(expectedMap, dependencyPathToThemesMap);

	}

	private static List<String> getSamplePartIFileLinesGeneToGene() {
		return Arrays.asList(
				"path	B	B.ind	W	W.ind	V+	V+.ind	E+	E+.ind	E	E.ind	I	I.ind	H	H.ind	Rg	Rg.ind	Q	Q.ind\n"
						+ "6|appos|start_entity mediated|nmod|6 mediated|nsubjpass|cleavage cleavage|compound|end_entity	0.000000	0	0.000000	0	1.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0\n"
						+ "detoxified|nmod|start_entity detoxified|ccomp|sensitized sensitized|nsubj|levels levels|nmod|abcg2 abcg2|nmod|cells cells|amod|end_entity	0.000000	0	333.000000	1	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	1.000000	0	1.000000	0\n"
						+ "vector|compound|start_entity transfected|nmod|vector transfected|nsubjpass|cells cells|compound|end_entity	0.000000	0	0.000000	0	0.000000	0	0.000000	0	0.000000	0	444.000000	1	0.000000	0	0.000000	0	2.000000	0\n"
						+ "cells|amod|start_entity inducing|dobj|cells role|acl|inducing had|dobj|role suggesting|ccomp|had incubated|parataxis|suggesting differentiate|advcl|incubated differentiate|nmod|end_entity	0.000000	0	0.000000	0	0.000000	0	1.000000	0	1.000000	0	0.000000	0	0.000000	0	0.000000	0	4.000000	0\n"
						+ "");
	}

	private File getSamplePartIFileGeneToGene() throws IOException {
		File file = folder.newFile();
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(file)),
				CharacterEncoding.UTF_8)) {
			for (String line : getSamplePartIFileLinesGeneToGene()) {
				writer.write(line);
			}
		}
		return file;
	}

	@Test
	public void testGetFlagshipPathsGeneToGene() throws IOException {
		List<Path> flagshipPaths = PerchaAltmanPartIFileParser.getFlagshipPaths(getSamplePartIFileGeneToGene());

		assertFalse(flagshipPaths.isEmpty());

		Path p1 = new Path(
				"detoxified|nmod|start_entity detoxified|ccomp|sensitized sensitized|nsubj|levels levels|nmod|abcg2 abcg2|nmod|cells cells|amod|end_entity",
				Arrays.asList(Theme.W_ENHANCES_RESPONSE), Arrays.asList(333.000000));
		Path p2 = new Path(
				"vector|compound|start_entity transfected|nmod|vector transfected|nsubjpass|cells cells|compound|end_entity",
				Arrays.asList(Theme.I_SIGNALING_PATHWAY), Arrays.asList(444.000000));
		List<Path> expectedFlagshipPaths = Arrays.asList(p1, p2);

		assertEquals(expectedFlagshipPaths, flagshipPaths);

	}

	@Test
	public void testGetDependencyPathToThemesMapGeneToGene() throws IOException {
		Map<String, Set<Theme>> dependencyPathToThemesMap = PerchaAltmanPartIFileParser
				.getDependencyPathToThemesMap(getSamplePartIFileGeneToGene());

		assertFalse(dependencyPathToThemesMap.isEmpty());

		Map<String, Set<Theme>> expectedMap = new HashMap<String, Set<Theme>>();

		expectedMap.put(
				"detoxified|nmod|start_entity detoxified|ccomp|sensitized sensitized|nsubj|levels levels|nmod|abcg2 abcg2|nmod|cells cells|amod|end_entity",
				CollectionsUtil.createSet(Theme.W_ENHANCES_RESPONSE));
		expectedMap.put(
				"vector|compound|start_entity transfected|nmod|vector transfected|nsubjpass|cells cells|compound|end_entity",
				CollectionsUtil.createSet(Theme.I_SIGNALING_PATHWAY));

		assertEquals(expectedMap, dependencyPathToThemesMap);

	}

}
