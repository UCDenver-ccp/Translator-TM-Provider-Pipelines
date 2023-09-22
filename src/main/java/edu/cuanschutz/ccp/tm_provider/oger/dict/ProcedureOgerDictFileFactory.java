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

	private static final List<String> EXCLUDED_ROOT_CLASSES = Arrays.asList();// "54709006" // body measurement
																				// (procedure)

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
						randomName, "procedure", false, null);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseSensWriter, dictLine);
			}

			for (String synonym : synonyms) {
				String dictLine = OgerDictFileFactory.getDictLine("SNOMEDCT", "SNOMEDCT:" + previousConceptId, synonym,
						randomName, "procedure", false, null);
				OgerDictFileFactory.writeDictLine(alreadyWritten, caseInsensWriter, dictLine);
			}
		}
	}

	private static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(Arrays.asList("243114000", // support
			"10012005", // expression
			"14788002", // sensitivity
			"119265000", // assisting
			"119265000", // assistant
			"118629009", // trains
			"118629009", // train
			"119271006", // obliterated
			"122467006", // fitness
			"122465003", // reconstructed
			"122464004", // augmented
			"122869004", // measurment
			"122869004", // measurement procedure
			"122546009", // stretching
			"122502001", // anchors
			"122502001", // anchor
			"14509009", // eviscerated
			"1431002", // attachment
			"1431002", // attach
			"19207007", // manipulate
			"18629005", // medical treatments
			"183376001", // mobilized
			"183376001", // mobilize
			"182832007", // medical management
			"169443000", // preventive
			"223482009", // discuss
			"223458004", // reporting
			"21147007", // clip
			"225313009", // supervision
			"225307000", // specialization
			"225414002", // generally observed
			"243115004", // family support
			"233546007", // needles
			"231097002", // cups
			"229824005", // positivity
			"229494005", // friction
			"229169001", // running in circles
			"250194009", // cells, phenotypically
			"252628008", // platform test
			"2677003", // stripping
			"252886007", // refractive
			"29513000", // syringes
			"28485005", // reinforcing
			"28485005", // reinforces
			"29703006", // complementation observed
			"304383000", // total proteins
			"313556000", // serum free fatty acid levels
			"313402005", // plasma amino acids levels
			"33230000", // activity test
			"32750006", // inspected
			"35860002", // pinned
			"35860002", // pin
			"35860002", // nails
			"33879002", // inoculating
			"33747003", // blood sugar levels
			"363778006", // phenotypical
			"387713003", // operation
			"386639001", // aborting
			"386476006", // touch
			"386453008", // support group
			"386053000", // assessments
			"373784005", // medical care
			"39250009", // enucleating
			"392230005", // needles
			"39857003", // weigh
			"410617001", // adjustable
			"410614008", // constructive
			"410538000", // scheduled
			"410025003", // clamped
			"41902000", // cross_match
			"4365001", // repaired
			"444635008", // digitally
			"67191004", // changes in bone length
			"781087000", // medical care
			"8378006", // trimming
			"91400004", // bisection
			"118629009", // instructs
			"118629009", // instruction
			"115979005", // stabilizes
			"115956009", // releases
			"122869004", // measurable
			"122545008", // stimulator
			"122502001", // anchored
			"122467006", // fits
			"122464004", // augment
			"14509009", // evidently
			"231287002", // infiltrate
			"229824005", // positionally
			"229057006", // meets
			"21147007", // clips
			"21147007", // clipping
			"19207007", // manipulating
			"257941004", // tubule
			"252886007" // refraction

	));

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
