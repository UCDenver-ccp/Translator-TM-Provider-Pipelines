package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.oger.dict.UtilityOgerDictFileFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultComplexSlotMention;

public class OgerPostProcessingFnTest {

	@Test
	public void testRemoveSpuriousMatches() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");

		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		idToOgerDictEntriesMap.put("PR:O74957", "Spom972h-ago1|PAZ Piwi domain protein ago1");
		idToOgerDictEntriesMap.put("PR:000012547", "Per1");

		TextAnnotation legitAnnot1 = factory.createAnnotation(0, 28, "PAZ Piwi domain protein ago1", "PR:O74957");
		TextAnnotation legitAnnot2 = factory.createAnnotation(0, 30, "PAZ Piwi domain protein (ago1)", "PR:O74957");
		TextAnnotation spuriousAnnot1 = factory.createAnnotation(0, 6, "ago [1", "PR:O74957");

		Set<TextAnnotation> allAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, spuriousAnnot1));
		Set<TextAnnotation> updatedAnnotations = OgerPostProcessingFn.removeSpuriousMatches(allAnnots,
				idToOgerDictEntriesMap);
		Set<TextAnnotation> expectedUpdatedAnnotations = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2));
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

		TextAnnotation spuriousAnnot2 = factory.createAnnotation(0, 4, "per", "PR:000012547");

		allAnnots = new HashSet<TextAnnotation>(Arrays.asList(spuriousAnnot2));
		updatedAnnotations = OgerPostProcessingFn.removeSpuriousMatches(allAnnots, idToOgerDictEntriesMap);
		expectedUpdatedAnnotations = new HashSet<TextAnnotation>(Arrays.asList());
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

		TextAnnotation spuriousAnnot3 = factory.createAnnotation(0, 4, "12.3", "PR:000012547");

		allAnnots = new HashSet<TextAnnotation>(Arrays.asList(spuriousAnnot3));
		updatedAnnotations = OgerPostProcessingFn.removeSpuriousMatches(allAnnots, idToOgerDictEntriesMap);
		expectedUpdatedAnnotations = new HashSet<TextAnnotation>(Arrays.asList());
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);

	}

	@Test
	public void testRemoveSpuriousMatches_UnexpectedGoCcExclusions() {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults("PMID:12345");

//		Map<String, Set<String>> idToOgerDictEntriesMap = new HashMap<String, Set<String>>();
//		idToOgerDictEntriesMap.put("GO:0030424", new HashSet<String>(Arrays.asList("axon")));
//		idToOgerDictEntriesMap.put("GO:0005764", new HashSet<String>(Arrays.asList("lysosome")));
//		idToOgerDictEntriesMap.put("GO:0005737", new HashSet<String>(Arrays.asList("cytoplasm")));

		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		idToOgerDictEntriesMap.put("GO:0030424", "axon");
		idToOgerDictEntriesMap.put("GO:0005764", "lysosome");
		idToOgerDictEntriesMap.put("GO:0005737", "cytoplasm");

		TextAnnotation legitAnnot1 = factory.createAnnotation(0, 5, "axons", "GO:0030424");
		TextAnnotation legitAnnot2 = factory.createAnnotation(0, 9, "lysosomal", "GO:0005764");
		TextAnnotation legitAnnot3 = factory.createAnnotation(0, 9, "cytoplasmic", "GO:0005737");

		Set<TextAnnotation> allAnnots = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, legitAnnot3));
		Set<TextAnnotation> updatedAnnotations = OgerPostProcessingFn.removeSpuriousMatches(allAnnots,
				idToOgerDictEntriesMap);
		Set<TextAnnotation> expectedUpdatedAnnotations = new HashSet<TextAnnotation>(
				Arrays.asList(legitAnnot1, legitAnnot2, legitAnnot3));
		assertEquals(expectedUpdatedAnnotations, updatedAnnotations);
	}

	/**
	 * trying to figure out why Enhanced S-cone syndrome appears to be excluded
	 * during post-processing
	 * 
	 * <pre>
	 * 		12345	cell	9	15	S-cone	S cone cell	CL:0003050		S1	CL
		12345	molecular_function	57	71	photoreceptors	photoreceptor activity	GO:0009881		S1	GO_MF
		12345	biological_process	233	248	dark adaptation	dark adaptation	GO:1990603		S1	GO_BP
		12345	biological_process	267	278	sensitivity	sensitization	GO:0046960		S1	GO_BP
		12345	disease	0	24	Enhanced S-cone syndrome	enhanced S-cone syndrome	MONDO:0100288		S1	MONDO
		12345	disease	86	101	night blindness	night blindness	MONDO:0004588		S1	MONDO
		12345	phenotype	86	101	night blindness	Nyctalopia	HP:0000662		S1	HP
		12345	phenotype	138	164	abnormal electroretinogram	Abnormal electroretinogram	HP:0000512		S1	HP
		12345	procedure	147	164	electroretinogram	ERG - electroretinography	SNOMEDCT:6615001		S1	SNOMEDCT
		12345	protein	166	169	ERG	PR:P81270	PR:000007173		S1	PR
		12345	procedure	267	278	sensitivity	Sensitivity	SNOMEDCT:14788002		S1	SNOMEDCT
		12345	protein	286	289	ERG	PR:P81270	PR:000007173		S1	PR
		12345	protein	26	30	ESCS	PR:Q9Y5X4	PR:000011403		S1	PR
		12345	protein	166	169	ERG	PR:000007173|PR:P11308	PR:000007173		S1	PR
		12345	protein	286	289	ERG	PR:000007173|PR:P11308	PR:000007173		S1	PR
	 * 
	 * </pre>
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testPostProcess() throws FileNotFoundException, IOException {

		String docId = "craft-16110338";
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		Map<String, String> idToOgerDictEntriesMap = new HashMap<String, String>();
		Collection<TextAnnotation> abbrevAnnots = new HashSet<TextAnnotation>();

		TextAnnotation abbrevAnnot1 = factory.createAnnotation(0, 24, "Enhanced S-cone syndrome", "long_form");
		TextAnnotation abbrevAnnot2 = factory.createAnnotation(26, 30, "ESCS", "short_form");
		DefaultComplexSlotMention csm = new DefaultComplexSlotMention("has_short_form");
		csm.addClassMention(abbrevAnnot2.getClassMention());
		abbrevAnnot1.getClassMention().addComplexSlotMention(csm);

		TextAnnotation abbrevAnnot3 = factory.createAnnotation(147, 164, "electroretinogram", "long_form");
		TextAnnotation abbrevAnnot4 = factory.createAnnotation(166, 169, "ERG", "short_form");
		DefaultComplexSlotMention csm2 = new DefaultComplexSlotMention("has_short_form");
		csm2.addClassMention(abbrevAnnot4.getClassMention());
		abbrevAnnot3.getClassMention().addComplexSlotMention(csm2);

		abbrevAnnots.add(abbrevAnnot1);
		abbrevAnnots.add(abbrevAnnot3);

		idToOgerDictEntriesMap.put("MONDO:0100288", "enhanced S-cone syndrome");

		String entries = "short-wavelength-sensitive (S) cone|S cone|short wavelength sensitive cone|S-cone photoreceptor|S-cone|short- (S) wavelength-sensitive cone|S cone cell|short-wavelength sensitive cone|S-(short-wavelength sensitive) cone";
		idToOgerDictEntriesMap.put("CL:0003050", entries);

		entries = "Nr2e3|RP37|rd7|UniProtKB:Q9QXZ7-1, 1-352|NR2E3|photoreceptor-specific nuclear receptor|mNR2E3/iso:m2|mNR2E3/iso:m1|PNR|RT06950p1|RNR|nuclear receptor subfamily 2 group E member 3|A930035N01Rik|photoreceptor-specific nuclear receptor isoform m2|photoreceptor-specific nuclear receptor isoform m1|mNR2E3|hNR2E3/iso:Short|Rp37|photoreceptor-specific nuclear receptor short form|photoreceptor-specific nuclear receptor long form|photoreceptor-specific nuclear receptor isoform Long|hormone receptor 51|hNR2E3/iso:Long|fly-NR2E3|photoreceptor-specific nuclear receptor isoform Short|nuclear receptor subfamily 2, group E, member 3|hNR2E3|ESCS|retina-specific nuclear receptor";
		idToOgerDictEntriesMap.put("PR:000011403", entries);

		entries = "photoreceptor|photoreceptor activity";
		idToOgerDictEntriesMap.put("GO:0009881", entries);

		entries = "nyctalopia|night blindness";
		idToOgerDictEntriesMap.put("MONDO:0004588", entries);

		entries = "Night blindness|Nyctalopia|Night-blindness|Poor night vision";
		idToOgerDictEntriesMap.put("HP:0000662", entries);

		entries = "Abnormal electroretinography|ERG abnormal|Abnormal ERG|Abnormal electroretinogram";
		idToOgerDictEntriesMap.put("HP:0000512", entries);

		entries = "Electroretinography|ERG - electroretinography|Electroretinogram|Electroretinography with medical evaluation|Electroretinogram with medical evaluation|ERG - Electroretinography|Electroretinography with medical evaluation (procedure)|Electroretinography (procedure)";
		idToOgerDictEntriesMap.put("SNOMEDCT:6615001", entries);

		entries = "mERG/iso:7|erg-3|hERG/iso:h6|transcriptional regulator ERG isoform h5|transcriptional regulator ERG isoform 1|p55|transcriptional regulator ERG isoform h6|transcriptional regulator ERG isoform 2|mERG/iso:exon4-containing|transcriptional regulator ERG isoform 5|transcriptional regulator ERG isoform 6|transcriptional regulator ERG isoform 3|transcriptional regulator ERG isoform 4|transcriptional regulator ERG isoform h4|transcriptional regulator ERG isoform ERG-2|transcriptional regulator ERG isoform 7|transcriptional regulator ERG isoform ERG-3|hERG/iso:ERG-3|hERG/iso:ERG-2|mERG|mERG/iso:exon3-containing|hERG|transforming protein ERG|transcriptional regulator ERG isoform ERG-1|D030036I24Rik|transcriptional regulator ERG|transcriptional regulator Erg|transcriptional regulator ERG isoform containing exon 3|transcriptional regulator ERG isoform containing exon 4|ETS transcription factor|ETS transcription factor ERG|chick-ERG|hERG/iso:h4|hERG/iso:h5|ERG/iso:4|hERG/iso:5|ERG/iso:3|mERG/iso:2|ERG/iso:1|mERG/iso:1|ERG|Erg|mERG/iso:6|mERG/iso:5|mERG/iso:3";
		idToOgerDictEntriesMap.put("PR:000007173", entries);

		idToOgerDictEntriesMap.put("GO:1990603", "dark adaptation");

		entries = "Antimicrobial susceptibility test (procedure)|Sensitivity|Antimicrobial susceptibility test|Antimicrobial susceptibility test, NOS|Sensitivities";
		idToOgerDictEntriesMap.put("SNOMEDCT:14788002", entries);

		idToOgerDictEntriesMap.put("GO:0046960", "sensitization");

		String documentText = "Enhanced S-cone syndrome (ESCS) is an unusual disease of photoreceptors that includes night blindness (suggestive of rod dysfunction), an abnormal electroretinogram (ERG) with a waveform that is nearly identical under both light and dark adaptation, and an increased sensitivity of the ERG to short-wavelength light."
				+ "\n" + UtilityOgerDictFileFactory.DOCUMENT_END_MARKER + "\n";

		TextAnnotation annot1 = factory.createAnnotation(0, 24, "Enhanced S-cone syndrome", "MONDO:0100288");
		TextAnnotation annot2 = factory.createAnnotation(9, 15, "S-cone", "CL:0003050");
		TextAnnotation annot3 = factory.createAnnotation(26, 30, "ESCS", "PR:000011403");
		TextAnnotation annot4 = factory.createAnnotation(57, 71, "photoreceptors", "GO:0009881");
		TextAnnotation annot5 = factory.createAnnotation(86, 101, "night blindness", "MONDO:0004588");
		TextAnnotation annot6 = factory.createAnnotation(86, 101, "night blindness", "HP:0000662");
		TextAnnotation annot7 = factory.createAnnotation(138, 164, "abnormal electroretinogram", "HP:0000512");
		TextAnnotation annot8 = factory.createAnnotation(147, 164, "electroretinogram", "SNOMEDCT:6615001");
		TextAnnotation annot9 = factory.createAnnotation(166, 169, "ERG", "PR:000007173");
		TextAnnotation annot10 = factory.createAnnotation(166, 169, "ERG", "PR:000007173");
		TextAnnotation annot11 = factory.createAnnotation(233, 248, "dark adaptation", "GO:1990603");
		TextAnnotation annot12 = factory.createAnnotation(267, 278, "sensitivity", "SNOMEDCT:14788002");
		TextAnnotation annot13 = factory.createAnnotation(267, 278, "sensitivity", "GO:0046960");
		TextAnnotation annot14 = factory.createAnnotation(286, 289, "ERG", "PR:000007173");
		TextAnnotation annot15 = factory.createAnnotation(286, 289, "ERG", "PR:000007173");
		annot1.setAnnotationID("annot1");
		annot2.setAnnotationID("annot2");
		annot3.setAnnotationID("annot3");
		annot4.setAnnotationID("annot4");
		annot5.setAnnotationID("annot5");
		annot6.setAnnotationID("annot6");
		annot7.setAnnotationID("annot7");
		annot8.setAnnotationID("annot8");
		annot9.setAnnotationID("annot9");
		annot10.setAnnotationID("annot10");
		annot11.setAnnotationID("annot11");
		annot12.setAnnotationID("annot12");
		annot13.setAnnotationID("annot13");
		annot14.setAnnotationID("annot14");
		annot15.setAnnotationID("annot15");

		Set<TextAnnotation> inputAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2, annot3, annot4,
				annot5, annot6, annot7, annot8, annot9, annot10, annot11, annot12, annot13, annot14, annot15));

		Set<TextAnnotation> postProcessedAnnots = OgerPostProcessingFn.removeSpuriousMatches(inputAnnots,
				idToOgerDictEntriesMap);

		Set<TextAnnotation> expectedPostProcessedAnnots = new HashSet<TextAnnotation>(Arrays.asList(annot1, annot2,
				annot3, annot4, annot5, annot6, annot7, annot8, annot9, annot10, annot11, annot12,
//				annot13, excluded b/c sensitivity is not close enough to sensitization,
				annot14, annot15));

		assertEquals(expectedPostProcessedAnnots, postProcessedAnnots);

	}

}