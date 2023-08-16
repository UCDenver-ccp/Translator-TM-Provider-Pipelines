package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

/**
 * TODO: implement this - from Drugbank XML file
 *
 */
public class ProcedureOgerDictFileFactory extends OgerDictFileFactory {

	private static final String PROCEDURE_SNOMED_ID = "71388002";

	private static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList();//"54709006" // body measurement (procedure)
	
	private final Set<String> procedureIdentifiers;

	public ProcedureOgerDictFileFactory(File snomedTransitiveSubclassRelationsFile)
			throws FileNotFoundException, IOException {
		super("procedure", "SNOMEDCT", SynonymSelection.EXACT_ONLY, EXCLUDED_ROOT_CLASSES);
		procedureIdentifiers = loadIdentifiers(snomedTransitiveSubclassRelationsFile);
	}

	/**
	 * procedure identifiers are in the 2nd column
	 * 
	 * @param snomedTransitiveSubclassRelationsFile
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private Set<String> loadIdentifiers(File snomedTransitiveSubclassRelationsFile)
			throws FileNotFoundException, IOException {

		Set<String> procedureIds = new HashSet<String>();
		StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(snomedTransitiveSubclassRelationsFile)),
				CharacterEncoding.UTF_8, "id\teffectiveTime");
		while (lineIter.hasNext()) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 100000 == 0) {
				System.out.println(String.format("progress: %d", line.getLineNumber()));
			}
			String[] cols = line.getText().split("\\t");
			String superTypeId = cols[0];
			if (superTypeId.equals(PROCEDURE_SNOMED_ID)) {
				String subTypeId = cols[1];
				procedureIds.add(subTypeId);
			}
		}

		return procedureIds;
	}

	@Override
	public void createOgerDictionaryFile(File snomedDescriptionsFile, File dictDirectory) throws IOException {

		File caseInsensitiveDictFile = new File(dictDirectory, "SNOMEDCT_PROCEDURES.case_insensitive.tsv");
		File caseSensitiveDictFile = new File(dictDirectory, "SNOMEDCT_PROCEDURES.case_sensitive.tsv");

		Set<String> alreadyWritten = new HashSet<String>();
		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensWriter = FileWriterUtil.initBufferedWriter(caseInsensitiveDictFile)) {

			StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(snomedDescriptionsFile)), CharacterEncoding.UTF_8,
					"id\teffectiveTime");
			String previousConceptId = null;
			Set<String> synonyms = new HashSet<String>();
			while (lineIter.hasNext()) {
				Line line = lineIter.next();
				if (line.getLineNumber() % 100000 == 0) {
					System.out.println(String.format("progress: %d", line.getLineNumber()));
				}
				String[] cols = line.getText().split("\\t");
				String conceptId = cols[4];
				if (previousConceptId == null) {
					previousConceptId = conceptId;
				}
				if (!conceptId.equals(previousConceptId)) {

					writeSynonymRecord(alreadyWritten, caseSensWriter, caseInsensWriter, previousConceptId, synonyms);
					synonyms = new HashSet<String>();
					previousConceptId = conceptId;

				}

				String synonym = cols[7];
				synonyms.add(synonym);

			}
			// write final concept (potentially)
			writeSynonymRecord(alreadyWritten, caseSensWriter, caseInsensWriter, previousConceptId, synonyms);
		}
	}

	/**
	 * If this is a procedure concept, then write the synonyms to the OGER dict
	 * files
	 * 
	 * @param alreadyWritten
	 * @param caseSensWriter
	 * @param caseInsensWriter
	 * @param previousConceptId
	 * @param synonyms
	 * @param conceptId
	 * @throws IOException
	 */
	private void writeSynonymRecord(Set<String> alreadyWritten, BufferedWriter caseSensWriter,
			BufferedWriter caseInsensWriter, String previousConceptId, Set<String> synonyms) throws IOException {
		if (procedureIdentifiers.contains(previousConceptId)) {

			// this is randomly selected b/c it doesn't matter what the name is -- just
			// needs to be a filler in the OGER dict file
			String randomName = synonyms.iterator().next();

			// then output the synonyms to file b/c this concept is a procedure
			synonyms = augmentSynonyms(previousConceptId, synonyms, null);
			/*
			 * split the synonyms into two sets, one that will be match in a case sensitive
			 * manner, and one that will be case insensitive
			 */
			Set<String> caseSensitiveSyns = OgerDictFileFactory.getCaseSensitiveSynonyms(synonyms);

			/* the synonyms set becomes the case-insensitive set */
			synonyms.removeAll(caseSensitiveSyns);

			for (String synonym : caseSensitiveSyns) {
				String dictLine = OgerDictFileFactory.getDictLine("SNOMEDCT", "SNOMEDCT:" + previousConceptId, synonym,
						randomName, "procedure", false);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseSensWriter, dictLine);
			}

			for (String synonym : synonyms) {
				String dictLine = OgerDictFileFactory.getDictLine("SNOMEDCT", "SNOMEDCT:" + previousConceptId, synonym,
						randomName, "procedure", false);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseInsensWriter, dictLine);
			}
		}
	}

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList("243114000", // support
			"10012005", // expression
			"14788002" // sensitivity
	));
	
	
//	snomedct:119265000 assisting
//	   1 snomedct:119265000 assistant
//	   1 snomedct:118629009 trains
//	   1 snomedct:118629009 train
//	   snomedct:119271006 obliterated
//	   snomedct:122467006 fitness
//	   1 snomedct:122465003 reconstructed
//	   1 snomedct:122464004 augmented
//	   snomedct:122869004 measurment
//	   1 snomedct:122869004 measurement procedure
//	   1 snomedct:122546009 stretching
//	   1 snomedct:122502001 anchors
//	   1 snomedct:122502001 anchor
//	   snomedct:14509009 eviscerated
//	   1 snomedct:1431002 attachment
//	   1 snomedct:1431002 attach
//	   1 snomedct:19207007 manipulate
//	   1 snomedct:18629005 medical treatments
//	   1 snomedct:183376001 mobilized
//	   1 snomedct:183376001 mobilize
//	   1 snomedct:182832007 medical management
//	   1 snomedct:169443000 preventive
//	   1 snomedct:223482009 discuss
//	   1 snomedct:223458004 reporting
//	   1 snomedct:21147007 clip
//	   1 snomedct:225313009 supervision
//	   1 snomedct:225307000 specialization
//	   1 snomedct:225414002 generally observed
//	   1 snomedct:243115004 family support
//	   1 snomedct:233546007 needles
//	   1 snomedct:231097002 cups
//	   1 snomedct:229824005 positivity
//	   1 snomedct:229494005 friction
//	   1 snomedct:229169001 running in circles
//	   snomedct:250194009 cells, phenotypically
//	   snomedct:252628008 platform test
//	   snomedct:2677003 stripping
//	   snomedct:252886007 refractive
//	   1 snomedct:29513000 syringes
//	   1 snomedct:28485005 reinforcing
//	   1 snomedct:28485005 reinforces
//	   snomedct:29703006 complementation observed
//	   snomedct:304383000 total proteins
//	   1 snomedct:313556000 serum free fatty acid levels
//	   1 snomedct:313402005 plasma amino acids levels
//	   snomedct:33230000 activity test
//	   1 snomedct:32750006 inspected	
//	   snomedct:35860002 pinned
//	   1 snomedct:35860002 pin
//	   1 snomedct:35860002 nails
//	   1 snomedct:33879002 inoculating
//	   1 snomedct:33747003 blood sugar levels
//	   snomedct:363778006 phenotypical
//	   snomedct:387713003 operation
//	   1 snomedct:386639001 aborting
//	   1 snomedct:386476006 touch
//	   1 snomedct:386453008 support group
//	   1 snomedct:386053000 assessments
//	   1 snomedct:373784005 medical care
//	   1 snomedct:39250009 enucleating
//	   1 snomedct:392230005 needles
//	   snomedct:39857003 weigh
//	   snomedct:410617001 adjustable
//	   1 snomedct:410614008 constructive
//	   1 snomedct:410538000 scheduled
//	   1 snomedct:410025003 clamped
//	   snomedct:41902000 cross_match
//	   snomedct:4365001 repaired
//	   snomedct:444635008 digitally
//	   snomedct:67191004 changes in bone length
//	   snomedct:781087000 medical care
//	   snomedct:8378006 trimming
//	   snomedct:91400004 bisection
//	   2 snomedct:118629009 instructs
//	   2 snomedct:118629009 instruction
//	   2 snomedct:115979005 stabilizes
//	   2 snomedct:115956009 releases
//	   2 snomedct:122869004 measurable
//	   2 snomedct:122545008 stimulator
//	   2 snomedct:122502001 anchored
//	   2 snomedct:122467006 fits
//	   2 snomedct:122464004 augment
//	   snomedct:14509009 evidently
//	   2 snomedct:231287002 infiltrate
//	   2 snomedct:229824005 positionally
//	   2 snomedct:229057006 meets
//	   2 snomedct:21147007 clips
//	   2 snomedct:21147007 clipping
//	   2 snomedct:19207007 manipulating
//	   2 snomedct:257941004 tubule
//	   2 snomedct:252886007 refraction
	   
	   
	   
	
	

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);
		toReturn = removeWordsLessThenLength(toReturn, 3);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put("363778006", new HashSet<String>(Arrays.asList("phenotype")));
		map.put("386397008", new HashSet<String>(Arrays.asList("presence")));
		map.put("363779003", new HashSet<String>(Arrays.asList("genotype")));
		map.put("4365001", new HashSet<String>(Arrays.asList("repair")));
		map.put("122501008", new HashSet<String>(Arrays.asList("fusion")));
		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}
}
