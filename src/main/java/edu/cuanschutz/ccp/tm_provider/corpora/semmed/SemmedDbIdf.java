package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import com.opencsv.CSVReader;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

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

		// this file generated from a query to our local database housing semmed
		File cui2LabelFile = new File(
				"/Users/bill/projects/ncats-translator/integrating-semmed/umls/semmed_cui_to_name.csv.gz");
//
//		// this file generated from a query to our local database housing semmed
//		File cui2pmidFile = new File(
//				"/Users/bill/projects/ncats-translator/integrating-semmed/semmed_cui_to_pmid.csv.gz");
//
//		// this file created from the UMLS MRREL.RRF file
//		File umlsIsaOwlFile = new File("/Users/bill/projects/ncats-translator/integrating-semmed/umls/umls_isa.owl");
//
		// we create this file here - it maps CUIs to PMID count
		File cui2pmidCountFile = new File(
				"/Users/bill/projects/ncats-translator/integrating-semmed/cuiToPmidCount.tsv.gz");
//
		// we create this file here - it maps CUIs to IDF
		File cui2idfFile = new File("/Users/bill/projects/ncats-translator/integrating-semmed/cuiToIdf.tsv.gz");

		File umlsMrconsoFile = new File(
				"/Users/bill/projects/ncats-translator/integrating-semmed/umls/2023AA/META/MRCONSO.RRF.gz");

//		// extracted from the ENTITY table file
//		File semmedSentToCuiFile = new File(
//				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/sent2cui.java.csv.gz");
//		// extracted from the SENTENCE table file
//		File semmedSentToPmidFile = new File(
//				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/sent2pmid.csv.gz");
//
//		File serializedCui2PmidMapFile = new File(
//				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/cui2pmidMap.ser");
//		File semmedCui2PmidFile = new File(
//				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/cui2pmid.2.csv.gz");
//
//		File semmedEntityTableFile = new File(
////				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/semmedVER43_2022_R_ENTITY.csv.gz");
//				"/Users/bill/projects/ncats-translator/integrating-semmed/2022/sample.csv.gz");

//		// setup for running in Docker
//		// input files
//		File cui2pmidFile = new File("/home/input/semmed_cui_to_pmid.csv.gz");
//		File umlsIsaOwlFile = new File("/home/input/umls_isa.owl");
//		File cui2LabelFile = new File("/home/input/semmed_cui_to_name.csv.gz");
//
//		// output files
//		File cui2pmidCountFile = new File("/home/input/cuiToPmidCount.tsv.gz");
//		File cui2idfFile = new File("/home/input/cuiToIdf.tsv.gz");

		try {

//			createCui2PmidFile(semmedEntityTableFile, semmedCui2PmidFile);

//			createCui2PmidFile(semmedSentToCuiFile, semmedSentToPmidFile, serializedCui2PmidMapFile);

//			createCui2PmidCountFile(cui2pmidFile, umlsIsaOwlFile, cui2pmidCountFile);
			computeIdf(cui2pmidCountFile, cui2LabelFile, cui2idfFile);
		} catch (IOException e) {
//		} catch (OWLOntologyCreationException | IOException e) {
			e.printStackTrace();
		}

		// original hacky attempt below
//		File cui2pmidCountFile = new File(
//				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/semmed_cui_pmid_counts.csv.gz");
//		File cui2idfFile = new File(
//				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/cui2idf.csv");
//
//		try {
//			computeIdf(cui2pmidCountFile, cui2idfFile);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		File umlsMrconsoFile = new File(
//				"/Users/bill/projects/ncats-translator/relay_feb_2023/semmed_session_results/semmed_idf/MRCONSO.ENG.RRF.gz");

	}

	private static void createCui2PmidFile(File semmedEntityTableFile, File semmedCuiToPmidFile)
			throws FileNotFoundException, IOException {
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(
				new GZIPOutputStream(new FileOutputStream(semmedCuiToPmidFile)), CharacterEncoding.UTF_8)) {
			StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(semmedEntityTableFile)), CharacterEncoding.UTF_8, null);

			try {
				while (lineIter.hasNext()) {
					Line line = lineIter.next();
					if (line.getLineNumber() % 1000000 == 0) {
						System.out.println(String.format("sent-to-pmid progress: %d", line.getLineNumber()));
					}

					if (line.getLineNumber() > 1370000000) {
						String[] cols = line.getText().split(",");
						String pmid = cols[2].replaceAll("\"", "");
						String cui = cols[3].replaceAll("\"", "");

						String l = String.format("%s,%s\n", cui, pmid);
						writer.write(l);
					}

				}
			} catch (Exception e) {
				// there is at least one line with a non-UTF-8 character
				System.err.println("Caught error.");
			}
		}

	}

	private static void createCui2PmidFile(File semmedEntityTableFile, File semmedSentenceTableFile,
			File serializedCui2PmidMapFile) throws FileNotFoundException, IOException {

		Map<String, String> sentenceIdToPmid = new HashMap<String, String>();

		// 237 million rows
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(semmedSentenceTableFile)), CharacterEncoding.UTF_8,
				null); lineIter.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 100000 == 0) {
				System.out.println(String.format("sent-to-pmid progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split(",");

			String sentenceId = cols[0];
			String pmid = cols[1];

			sentenceIdToPmid.put(sentenceId, pmid);
		}

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
//		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(
//				new GZIPOutputStream(new FileOutputStream(cui2pmidFile)), CharacterEncoding.UTF_8)) {

		// 1,723,225,175 rows
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(semmedEntityTableFile)), CharacterEncoding.UTF_8,
				null); lineIter.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 100000 == 0) {
				System.out.println(String.format("cui-to-pmid progress: %d", line.getLineNumber()));
			}
			String[] cols = line.getText().split(",");
			String sentenceId = cols[0];
			String cui = cols[1];

			if (sentenceIdToPmid.containsKey(sentenceId)) {
				String pmid = sentenceIdToPmid.get(sentenceId);
				CollectionsUtil.addToOne2ManyUniqueMap(cui, pmid, map);
			} else {
				System.out.println("missing sentence id: " + sentenceId);
			}

//				writer.write(String.format("%s\t%s\n", cui, sentenceId));

		}
//		}

		FileOutputStream fileOutputStream = new FileOutputStream(serializedCui2PmidMapFile);
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
		objectOutputStream.writeObject(map);
		objectOutputStream.flush();
		objectOutputStream.close();

	}

	public static void createCui2PmidCountFile(File cui2pmidFile, File umlsIsaOwlFile, File outputFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {

		System.out.println("Loading UMLS IS-A ontology...");
		OntologyUtil ontUtil = new OntologyUtil(umlsIsaOwlFile);

		System.out.println("Loading CUI-to-PMID map...");
		Map<String, Set<String>> cuiToPmidMap = loadCuiToPmidMap(cui2pmidFile);

		Set<String> cuis = new HashSet<String>(cuiToPmidMap.keySet());

		System.out.println("Augmenting ancestors in CUI-to-PMID map...");
		int count = 0;
		for (String cui : cuis) {
			if (count++ % 1000 == 0) {
				System.out.println(String.format("progress: %d of %d", count - 1, cuis.size()));
			}
			// for each key, get it's associated pmids
			Set<String> pmids = cuiToPmidMap.get(cui);

			// get all ancestors of the key class
			OWLClass cls = ontUtil.getOWLClassFromId(CreateUmlsSubclassOntology.ONTOLOGY_IRI + "/" + cui);
			if (cls != null) {
				Set<OWLClass> ancestors = ontUtil.getAncestors(cls);

				// add all of the key-associated to each of its ancestors
				for (OWLClass ancestor : ancestors) {
					String ancestorCui = getCuiFromIri(ancestor.getIRI());
					if (cuiToPmidMap.containsKey(ancestorCui)) {
						cuiToPmidMap.get(ancestorCui).addAll(pmids);
					} else {
						cuiToPmidMap.put(ancestorCui, new HashSet<String>(pmids));
					}
				}
			} else {
//				System.out.println(String.format("Class not found in ontology: %s",
//						CreateUmlsSubclassOntology.ONTOLOGY_IRI + "/" + cui));
			}

		}

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {
			for (Entry<String, Set<String>> entry : cuiToPmidMap.entrySet()) {
				writer.write(String.format("%s,%d\n", entry.getKey(), entry.getValue().size()));
			}
		}

	}

	private static Map<String, Set<String>> loadCuiToPmidMap(File cui2pmidFile)
			throws FileNotFoundException, IOException {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(cui2pmidFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}
			String[] cols = line.getText().replaceAll("\"", "").split(",");
			String cui = cols[0];
			String pmid = cols[1];

			CollectionsUtil.addToOne2ManyUniqueMap(cui, pmid, map);

//			if (line.getLineNumber() == 1000) {
//				break;
//			}
		}
		return map;
	}

	/**
	 * Extract the CUI from the IRI
	 * 
	 * @param iri
	 * @return
	 */
	private static String getCuiFromIri(IRI iri) {
		String iriStr = iri.toString();
		return StringUtil.removePrefix(iriStr, CreateUmlsSubclassOntology.ONTOLOGY_IRI + "/");
	}

	public static void computeIdf(File cui2pmidCountFile, File cui2labelFile, File outputFile)
			throws FileNotFoundException, IOException {

		Map<String, String> cuiToLabelMap = loadCuiToLabelMap(cui2labelFile);

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
			double pmidCount = Double.parseDouble(nextRecord[1]);

			double idf = Math.log(pmidCount / TOTAL_PMIDS);

			String name = cuiToLabelMap.get(cui);

			cui2idfMap.put(String.format("%s|%s", cui, name), idf);
		}

		Map<String, Double> sortedMap = CollectionsUtil.sortMapByValues(cui2idfMap, SortOrder.DESCENDING);

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {
			writer.write("cui\tname\tidf\n");
			for (Entry<String, Double> entry : sortedMap.entrySet()) {
				String[] cols = entry.getKey().split("\\|");
				writer.write(String.format("%s\t\"%s\"\t%4.3f\n", cols[0], cols[1], entry.getValue()));
			}
		}

	}

	private static Map<String, String> loadCuiToLabelMapMRCONSO(File mrconsoFile)
			throws FileNotFoundException, IOException {
		Map<String, String> map = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(mrconsoFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split("\\|");
			String cui = cols[0];
			String label = cols[14];

			map.put(cui, label);
		}
		return map;
	}

	private static Map<String, String> loadCuiToLabelMap(File cui2labelFile) throws FileNotFoundException, IOException {
		Map<String, String> map = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(cui2labelFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() % 10000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split(",");
			String cui = cols[0].replaceAll("\"", "");
			String label = cols[1].replaceAll("\"", "");

			map.put(cui, label);
		}
		return map;
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
