package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.opencsv.CSVReader;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

/**
 * Computes Inverse Document Frequency for all UMLS CUIs used as subject and/or
 * object entities in SemMedDB predications.
 * 
 * Note: The initial version does not take into account any hierarchical
 * relationships, and therefore this computation is not optimal.
 *
 * We will do most of the work using a SQL query that joins the entity and
 * sentence tables and exports PMID counts for each CUI
 * 
 * 
 * 
 * 
 * 
 * 
 */
public class SemmedDbIdf {

	private static final double TOTAL_PMIDS = 34315455.0;

	public static void main(String[] args) {
		File cui2pmidCountFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/semmed_cui_pmid_counts.csv.gz");
		File cui2idfFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/cui2idf.csv");

		try {
			computeIdf(cui2pmidCountFile, cui2idfFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		File umlsMrconsoFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/MRCONSO.ENG.RRF.gz");

	}

	public static void computeIdf(File cui2pmidCountFile, File outputFile) throws FileNotFoundException, IOException {
		Map<String, Double> cui2idfMap = new HashMap<String, Double>();

		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(cui2pmidCountFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();

			// skip the first line b/c the cui is blank
			if (line.getLineNumber() == 0) {
				continue;
			}

			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %d\n", line.getLineNumber()));
				continue;
			}

			String cui = nextRecord[0];
			String name = nextRecord[1];
			double pmidCount = Double.parseDouble(nextRecord[2]);

			double idf = Math.log(pmidCount / TOTAL_PMIDS);

			cui2idfMap.put(String.format("%s|%s", cui, name), idf);
		}

		Map<String, Double> sortedMap = CollectionsUtil.sortMapByValues(cui2idfMap, SortOrder.DESCENDING);

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			writer.write("cui, name,idf\n");
			for (Entry<String, Double> entry : sortedMap.entrySet()) {
				String[] cols = entry.getKey().split("\\|");
				writer.write(String.format("%s,\"%s\",%4.3f\n", cols[0], cols[1], entry.getValue()));
			}
		}

	}

	/**
	 * 
	 * NOTE: this is incomplete. In order to do this we need the SQL to return the
	 * actual PMIDs so we can combine PMID sets appropriately when incorporating the
	 * subclass hierarchies.
	 * 
	 * @param cui2pmidCountFile
	 * @param UmlsMrconsoFile
	 * @param hpoFile
	 * @param outputFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void computeIdfWithHierarchies(File cui2pmidCountFile, File UmlsMrconsoFile, File hpoFile,
			File outputFile) throws FileNotFoundException, IOException {
		Map<String, Double> cui2idfMap = new HashMap<String, Double>();

		Map<String, Set<String>> umls2HpoMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> hpo2UmlsMap = new HashMap<String, Set<String>>();
		populateUmlsToHpoMaps(UmlsMrconsoFile, umls2HpoMap, hpo2UmlsMap);

		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(cui2pmidCountFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();

			// skip the first line b/c the cui is blank
			if (line.getLineNumber() == 0) {
				continue;
			}

			String[] nextRecord = null;
			try {
				Reader lineReader = new StringReader(line.getText());
				CSVReader csvReader = new CSVReader(lineReader);
				nextRecord = csvReader.readNext();
				csvReader.close();
			} catch (Exception e) {
				System.err.println(String.format("CSV parse error. Line %d\n", line.getLineNumber()));
				continue;
			}

			String cui = nextRecord[0];
			String name = nextRecord[1];
			double pmidCount = Double.parseDouble(nextRecord[2]);

			double idf = Math.log(pmidCount / TOTAL_PMIDS);

			cui2idfMap.put(String.format("%s|%s", cui, name), idf);
		}

		Map<String, Double> sortedMap = CollectionsUtil.sortMapByValues(cui2idfMap, SortOrder.DESCENDING);

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			writer.write("cui, name,idf\n");
			for (Entry<String, Double> entry : sortedMap.entrySet()) {
				String[] cols = entry.getKey().split("\\|");
				writer.write(String.format("%s,\"%s\",%4.3f\n", cols[0], cols[1], entry.getValue()));
			}
		}

	}

	/**
	 * populates maps from umls-to-hpo and hpo-to-umls from the UMLS MRCONSO file
	 * 
	 * @param umlsMrconsoFile
	 * @param umls2HpoMap
	 * @param hpo2UmlsMap
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void populateUmlsToHpoMaps(File umlsMrconsoFile, Map<String, Set<String>> umls2HpoMap,
			Map<String, Set<String>> hpo2UmlsMap) throws FileNotFoundException, IOException {
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(umlsMrconsoFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("Progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split("\\|");
			String cui = cols[0];

			String sourceName = cols[11];
			if (sourceName.equals("HPO")) {
				String sourceCode = cols[13];

				CollectionsUtil.addToOne2ManyUniqueMap(cui, sourceCode, umls2HpoMap);
				CollectionsUtil.addToOne2ManyUniqueMap(sourceCode, cui, hpo2UmlsMap);

			}
		}

	}

}
