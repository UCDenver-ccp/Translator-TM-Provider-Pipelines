package edu.cuanschutz.ccp.tm_provider.relation_extraction.bicluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Data;

public class PerchaAltmanPartIFileParser {

	public enum Theme {
		// chemical-gene
		Aplus_AGONISM("A+"), Aminus_ANTAGONISM("A-"), B_BINDING("B"), Eplus_INCREASES_EXPRESSION("E+"),
		Eminus_DECREASES_EXPRESSION("E-"), E_AFFECTS_EXPRESSION("E"), N_INHIBITS("N"),

		// gene-chemical
		O_TRANSPORTS("O"), K_METABOLISM("K"), Z_ENZYME_ACTIVITY("Z"),

		// gene-gene
		W_ENHANCES_RESPONSE("W"), Vplus_ACTIVATES("V+"), I_SIGNALING_PATHWAY("I"), H_SAME_PROTEIN_OR_COMPLEX("H"),
		Rg_REGULATION("Rg"), Q_PRODUCTION_BY_CELL_POPULATION("Q"),

		// chemical-disease
		T_TREATMENT("T"), C_INHIBITS_CELL_GROWTH("C"), Sa_SIDE_EFFECT("Sa"), Pr_PREVENTS("Pr"), Pa_ALLEVIATES("Pa"),
		J_ROLE_IN_PATHOGENESIS("J"),

		// disease-chemical
		Mp_BIOMARKERS_PROGRESSION("Mp"),

		// gene-disease
		U_CAUSAL_MUTATIONS("U"), Ud_MUTATIONS_AFFECT_DISEASE_COURSE("Ud"), D_DRUG_TARGETS("D"),
		Te_THERAPEUTIC_EFFECT("Te"), Y_POLYMORPHISMS_ALTER_RISK("Y"), G_PROMOTES_PROGRESSION("G"),

		// disease-gene
		Md_BIOMARKERS_DIAGNOSTIC("Md"), X_OVEREXPRESSION_IN_DISEASE("X"), L_IMPROPER_REGULATION_LINKED_TO_DISEASE("L");

		private final String symbol;

		private Theme(String symbol) {
			this.symbol = symbol;
		}

		public String getSymbol() {
			return this.symbol;
		}
	}

	@Data
	public static class Path {
		private final String path;
		private final List<Theme> themes;
		private final List<Double> scores;
	}

	@Data
	private static class ThemeScore {
		private final List<Theme> themes = new ArrayList<Theme>();
		private final List<Double> scores = new ArrayList<Double>();

		public void addTheme(Theme t) {
			themes.add(t);
		}

		public void addScore(Double s) {
			scores.add(s);
		}

		public void addThemeScore(Theme t, Double s) {
			addTheme(t);
			addScore(s);
		}

	}

	public static List<Path> getFlagshipPaths(File partIFile) throws IOException {
		List<Path> paths = new ArrayList<Path>();

		List<Theme> fileThemes = null;
		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(partIFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() == 0) {
				fileThemes = getFileThemes(line.getText());
				continue;
			}

			if (line.getLineNumber() % 10000 == 0) {
				System.out.println("progress -- " + line.getLineNumber());
			}

			String[] cols = line.getText().split("\\t");
			String dependencyPath = cols[0];

			ThemeScore ts = getFlagshipThemeScore(cols, fileThemes);

			if (ts != null) {
				paths.add(new Path(dependencyPath, ts.getThemes(), ts.getScores()));
			}
		}

		System.out.println("path count: " + paths.size());

		return paths;
	}

	/**
	 * Parses the header line and returns an ordered list of Themes as they appear
	 * in the file header
	 * 
	 * @param text
	 * @return
	 */
	private static List<Theme> getFileThemes(String headerText) {
		Map<String, Theme> symbolToThemeMap = getSymbolToThemeMap();

		List<Theme> themes = new ArrayList<Theme>();
		String[] cols = headerText.split("\\t");

		for (int i = 1; i < cols.length; i += 2) {
			themes.add(symbolToThemeMap.get(cols[i]));
		}

		return themes;
	}

	/**
	 * @return a mapping from a Theme's symbol to the Theme itself. The symbol is
	 *         found in the file header of the part i files.
	 */
	private static Map<String, Theme> getSymbolToThemeMap() {
		Map<String, Theme> symbolToThemeMap = new HashMap<String, Theme>();

		for (Theme t : Theme.values()) {
			symbolToThemeMap.put(t.getSymbol(), t);
		}

		return symbolToThemeMap;
	}

	/**
	 * Interrogate the input array of columns from the part-i-* file. If there is a
	 * flagship theme, return this dependency path, else return null.
	 * 
	 * Note: It is possible for a dependency path to have multiple themes, e.g.
	 * Eplus_INCREASES_EXPRESSION & E_AFFECTS_EXPRESSION
	 * 
	 * @param cols
	 * @param list of themes in the order they appear in the file header
	 * @return
	 */
	private static ThemeScore getFlagshipThemeScore(String[] cols, List<Theme> fileThemes) {
		int index = 2;
		ThemeScore ts = null;
		for (Theme theme : fileThemes) {
			if (Integer.parseInt(cols[index]) > 0) {
				if (ts == null) {
					ts = new ThemeScore();
					ts.addThemeScore(theme, Double.parseDouble(cols[index - 1]));
				} else {
					ts.addThemeScore(theme, Double.parseDouble(cols[index - 1]));
				}
			}
			index += 2;
		}
		return ts;
	}

	public static Map<String, Set<Theme>> getDependencyPathToThemesMap(File partIFile) throws IOException {
		List<Path> flagshipPaths = getFlagshipPaths(partIFile);

		Map<String, Set<Theme>> pathToThemeMap = new HashMap<String, Set<Theme>>();
		for (Path p : flagshipPaths) {
			for (Theme t : p.getThemes()) {
				CollectionsUtil.addToOne2ManyUniqueMap(p.getPath(), t, pathToThemeMap);
			}
		}
		return pathToThemeMap;

	}

	public static void main(String[] args) {
		File partIFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/bicluster/part-i-chemical-gene-path-theme-distributions.txt.gz");
				"/Users/bill/projects/ncats-translator/relations/bicluster/gene-gene/part-i-gene-gene-path-theme-distributions.txt.gz");

		try {
			List<Path> flagshipPaths = getFlagshipPaths(partIFile);

			// sort flagship paths by theme
			Map<Theme, Set<Path>> theme2PathMap = sortPathsByTheme(flagshipPaths);

			System.out.println();
			Map<Theme, Set<Path>> sortedMap = CollectionsUtil.sortMapByKeys(theme2PathMap, SortOrder.ASCENDING);
			for (Entry<Theme, Set<Path>> entry : sortedMap.entrySet()) {
				System.out.println(entry.getKey().name() + " -- " + entry.getValue().size());
			}

			System.out.println();
			Map<String, Set<Path>> themeStrToPathsMap = createThemeStrToPathsMap(flagshipPaths);
			Map<String, Set<Path>> sortedMap2 = CollectionsUtil.sortMapByKeys(themeStrToPathsMap, SortOrder.DESCENDING);
			for (Entry<String, Set<Path>> entry : sortedMap2.entrySet()) {
				System.out.println(entry.getKey() + " -- " + entry.getValue().size());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static Map<String, Set<Path>> createThemeStrToPathsMap(List<Path> paths) {
		Map<String, Set<Path>> map = new HashMap<String, Set<Path>>();

		for (Path p : paths) {
			String themeString = getThemeStr(p.getThemes());
			CollectionsUtil.addToOne2ManyUniqueMap(themeString, p, map);
		}

		return map;
	}

	private static String getThemeStr(List<Theme> themes) {
		List<String> s = new ArrayList<String>();
		for (Theme t : themes) {
			s.add(t.name());
		}
		Collections.sort(s);
		return CollectionsUtil.createDelimitedString(s, ";");
	}

	/**
	 * @param flagshipPaths
	 * @return a map from theme to path
	 */
	private static Map<Theme, Set<Path>> sortPathsByTheme(List<Path> paths) {
		Map<Theme, Set<Path>> theme2PathMap = new HashMap<Theme, Set<Path>>();
		for (Path p : paths) {
			for (Theme t : p.getThemes()) {
				CollectionsUtil.addToOne2ManyUniqueMap(t, p, theme2PathMap);
			}
		}
		return theme2PathMap;
	}
}
