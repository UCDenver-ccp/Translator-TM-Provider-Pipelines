package edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

public class BatchCreateCommandTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private static void populateConceptIdfFile(File conceptIdfFile) throws IOException {
		List<String> lines = Arrays
				.asList("\"CHEBI:100147\",\"document\",14.5569532\n" + "\"CHEBI:10033\",\"document\",11.3490464\n"
						+ "\"GO:0000003\",\"document\",6.1524809\n" + "\"GO:0000009\",\"document\",13.4583409\n");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(
				new GZIPOutputStream(new FileOutputStream(conceptIdfFile)), CharacterEncoding.UTF_8)) {
			FileWriterUtil.printLines(lines, writer);
		}

	}

	/**
	 * 
	 * IDF threshold = -1 so should include the input classes regardless of IDF
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testAddConceptIdsAboveIdfThreshold() throws FileNotFoundException, IOException {
		File conceptIdfFile = folder.newFile("conceptIdf.gz");
		populateConceptIdfFile(conceptIdfFile);

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		float idfThreshold = -1.0f;
		Set<String> ontologyPrefixes = null;
		Set<String> inputClassIds = new HashSet<String>(Arrays.asList("GO:0000003", "GO:0000009"));
		BatchCreateCommand.addConceptIdsAboveIdfThreshold(map, idfThreshold, ontologyPrefixes, inputClassIds,
				conceptIdfFile);

		Map<String, Set<String>> expectedMap = new HashMap<String, Set<String>>();
		CollectionsUtil.addToOne2ManyUniqueMap("GO", "GO:0000003", expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap("GO", "GO:0000009", expectedMap);

		assertEquals("IDF threshold = -1 so should include the input classes regardless of IDF", expectedMap, map);
	}

	/**
	 * 
	 * IDF threshold > 0 so should filter the input classes based on IDF
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testAddConceptIdsAboveIdfThreshold1() throws FileNotFoundException, IOException {
		File conceptIdfFile = folder.newFile("conceptIdf.gz");
		populateConceptIdfFile(conceptIdfFile);

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		float idfThreshold = 8.0f;
		Set<String> ontologyPrefixes = null;
		Set<String> inputClassIds = new HashSet<String>(Arrays.asList("GO:0000003", "GO:0000009"));
		BatchCreateCommand.addConceptIdsAboveIdfThreshold(map, idfThreshold, ontologyPrefixes, inputClassIds,
				conceptIdfFile);

		Map<String, Set<String>> expectedMap = new HashMap<String, Set<String>>();
		CollectionsUtil.addToOne2ManyUniqueMap("GO", "GO:0000009", expectedMap);

		assertEquals("IDF threshold > 0 so should filter the input classes based on IDF", expectedMap, map);
	}

	/**
	 * 
	 * IDF threshold < 0 and no input class ids, so expected map should be empty
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testAddConceptIdsAboveIdfThreshold2() throws FileNotFoundException, IOException {
		File conceptIdfFile = folder.newFile("conceptIdf.gz");
		populateConceptIdfFile(conceptIdfFile);

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		float idfThreshold = -1.0f;
		Set<String> ontologyPrefixes = new HashSet<String>(Arrays.asList("CHEBI", "GO"));
		Set<String> inputClassIds = null;
		BatchCreateCommand.addConceptIdsAboveIdfThreshold(map, idfThreshold, ontologyPrefixes, inputClassIds,
				conceptIdfFile);

		assertTrue("IDF threshold < 0 and no input class ids, so expected map should be empty", map.isEmpty());
	}

	/**
	 * 
	 * IDF threshold > 0 and no input class ids, so expected map should contain
	 * classes that pass the IDF threshold and have prefixes from the ontology
	 * prefix set
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testAddConceptIdsAboveIdfThreshold3() throws FileNotFoundException, IOException {
		File conceptIdfFile = folder.newFile("conceptIdf.gz");
		populateConceptIdfFile(conceptIdfFile);

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		float idfThreshold = 12.0f;
		Set<String> ontologyPrefixes = new HashSet<String>(Arrays.asList("CHEBI", "GO"));
		Set<String> inputClassIds = null;
		BatchCreateCommand.addConceptIdsAboveIdfThreshold(map, idfThreshold, ontologyPrefixes, inputClassIds,
				conceptIdfFile);

		Map<String, Set<String>> expectedMap = new HashMap<String, Set<String>>();
		CollectionsUtil.addToOne2ManyUniqueMap("CHEBI", "CHEBI:100147", expectedMap);
		CollectionsUtil.addToOne2ManyUniqueMap("GO", "GO:0000009", expectedMap);

		assertEquals(
				"IDF threshold > 0 and no input class ids, so expected map should contain "
						+ "classes that pass the IDF threshold and have prefixes from the ontology prefix set",
				expectedMap, map);
	}

	/**
	 * 
	 * IDF threshold > 0 and no input class ids, so expected map should contain
	 * classes that pass the IDF threshold and have prefixes from the ontology
	 * prefix set
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void testAddConceptIdsAboveIdfThreshold4() throws FileNotFoundException, IOException {
		File conceptIdfFile = folder.newFile("conceptIdf.gz");
		populateConceptIdfFile(conceptIdfFile);

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		float idfThreshold = 12.0f;
		Set<String> ontologyPrefixes = new HashSet<String>(Arrays.asList("CHEBI"));
		Set<String> inputClassIds = null;
		BatchCreateCommand.addConceptIdsAboveIdfThreshold(map, idfThreshold, ontologyPrefixes, inputClassIds,
				conceptIdfFile);

		Map<String, Set<String>> expectedMap = new HashMap<String, Set<String>>();
		CollectionsUtil.addToOne2ManyUniqueMap("CHEBI", "CHEBI:100147", expectedMap);

		assertEquals(
				"IDF threshold > 0 and no input class ids, so expected map should contain "
						+ "classes that pass the IDF threshold and have prefixes from the ontology prefix set",
				expectedMap, map);
	}

}
