package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.AssertionTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.AssertionTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EntityTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EntityTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceScoreTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceScoreTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;

public class ClassifiedSentenceStorageSqlValuesFnTest {

	@Test
	public void testAssertionTableValuesCoder() throws IOException {
		String assertionId = "1123jfb5k6b3";
		String subjectCurie = "PR:0000000";
		String objectCurie = "CL:0000000";
		String associationCurie = "biolink:located_in";
		AssertionTableValues atv = new AssertionTableValues(assertionId, subjectCurie, objectCurie, associationCurie);

		AssertionTableValuesCoder atvCoder = new AssertionTableValuesCoder();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		atvCoder.encode(atv, outStream);

		InputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
		AssertionTableValues decodedAtv = atvCoder.decode(inStream);

		assertEquals(atv, decodedAtv);
	}

	@Test
	public void testEvidenceTableValuesCoder() throws IOException {
		String assertionId = "1123jfb5k6b3";
		String evidenceId = "89797dgasgd97";
		String documentId = "PMID:1234";
		String sentence = "This is a sentence.";
		String subjectEntityId = "231251jkhjkh";
		String objectEntityId = "45h4hv1vk45";
		String documentZone = "abstract";
		String documentPublicationTypesStr = "journal article";
		int documentYearPublished = 1999;
		EvidenceTableValues etv = new EvidenceTableValues(evidenceId, assertionId, documentId, sentence,
				subjectEntityId, objectEntityId, documentZone, documentPublicationTypesStr, documentYearPublished);

		EvidenceTableValuesCoder etvCoder = new EvidenceTableValuesCoder();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		etvCoder.encode(etv, outStream);

		InputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
		EvidenceTableValues decodedEtv = etvCoder.decode(inStream);

		assertEquals(etv, decodedEtv);
	}

	@Test
	public void testEntityTableValuesCoder() throws IOException {
		String entityId = "4512lkj13lk5";
		String span = "34|114";
		String coveredText = "covered text";
		EntityTableValues etv = new EntityTableValues(entityId, span, coveredText);

		EntityTableValuesCoder coder = new EntityTableValuesCoder();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		coder.encode(etv, outStream);

		InputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
		EntityTableValues decodedAtv = coder.decode(inStream);

		assertEquals(etv, decodedAtv);
	}

	@Test
	public void testEvidenceScoreTableValuesCoder() throws IOException {
		String evidenceId = "124235hgg1l5";
		String predicateCurie = "biolink:located_in";
		double score = 0.99945256832;
		EvidenceScoreTableValues esv = new EvidenceScoreTableValues(evidenceId, predicateCurie, score);

		EvidenceScoreTableValuesCoder coder = new EvidenceScoreTableValuesCoder();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		coder.encode(esv, outStream);

		InputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
		EvidenceScoreTableValues decodedEsv = coder.decode(inStream);

		assertEquals(esv, decodedEsv);
	}

	@Test
	public void testProcessLines() {
		BiolinkAssociation biolinkAssoc = BiolinkAssociation.BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE;
		double bertScoreInclusionMinimumThreshold = 0.9;
		String bertOutputLine = "108be6abe12d0fa43eda98a11db0f64601bc27fc67327cbdd2a6e23ecbc985e7\t@CHEMICAL$ and ciprofloxacin were started to treat suspected community-acquired @DISEASE$.\t0.9994267\t0.00019880748\t0.0003744";
		String metadataLine = "108be6abe12d0fa43eda98a11db0f64601bc27fc67327cbdd2a6e23ecbc985e7\t@CHEMICAL$ and ciprofloxacin were started to treat suspected community-acquired @DISEASE$.\tPMC6940177\tCeftriaxone\tDRUGBANK:DB01212\t0|11\tpneumonia\tMONDO:0005249\t81|90\t\t91\t\tCeftriaxone and ciprofloxacin were started to treat suspected community-acquired pneumonia.\tCASE\t\t2155";
		List<AssertionTableValues> assertionTableValues = new ArrayList<AssertionTableValues>();
		List<EvidenceTableValues> evidenceTableValues = new ArrayList<EvidenceTableValues>();
		List<EntityTableValues> entityTableValues = new ArrayList<EntityTableValues>();
		List<EvidenceScoreTableValues> evidenceScoreTableValues = new ArrayList<EvidenceScoreTableValues>();
		ClassifiedSentenceStorageSqlValuesFn.processLines(biolinkAssoc, bertScoreInclusionMinimumThreshold,
				bertOutputLine, metadataLine, assertionTableValues, evidenceTableValues, entityTableValues,
				evidenceScoreTableValues);

		List<AssertionTableValues> expectedAssertionTableValues = new ArrayList<AssertionTableValues>();
		List<EvidenceTableValues> expectedEvidenceTableValues = new ArrayList<EvidenceTableValues>();
		List<EntityTableValues> expectedEntityTableValues = new ArrayList<EntityTableValues>();
		List<EvidenceScoreTableValues> expectedEvidenceScoreTableValues = new ArrayList<EvidenceScoreTableValues>();

		String subjectCurie = "DRUGBANK:DB01212";
		String objectCurie = "MONDO:0005249";
		String expectedAssertionId = ClassifiedSentenceStorageSqlValuesFn.getAssertionId(biolinkAssoc, subjectCurie,
				objectCurie);
		AssertionTableValues expectedAvt = new AssertionTableValues(expectedAssertionId, subjectCurie, objectCurie,
				biolinkAssoc.getAssociationId());
		expectedAssertionTableValues.add(expectedAvt);

		String subjectSpanStr = "0|11";
		String objectSpanStr = "81|90";
		String documentId = "PMC6940177";
		String sentence = "Ceftriaxone and ciprofloxacin were started to treat suspected community-acquired pneumonia.";
		String evidenceId = ClassifiedSentenceStorageSqlValuesFn.getEvidenceId(biolinkAssoc, subjectSpanStr,
				objectSpanStr, documentId, sentence, subjectCurie, objectCurie);
		String subjectEntityId = ClassifiedSentenceStorageSqlValuesFn.getEntityId(biolinkAssoc, subjectSpanStr,
				documentId, sentence, subjectCurie);
		String objectEntityId = ClassifiedSentenceStorageSqlValuesFn.getEntityId(biolinkAssoc, objectSpanStr,
				documentId, sentence, objectCurie);
		String documentZone = "CASE";
		String documentPublicationTypesStr = "";
		int documentYearPublished = 2155;
		EvidenceTableValues expectedEvt = new EvidenceTableValues(evidenceId, expectedAssertionId, documentId, sentence,
				subjectEntityId, objectEntityId, documentZone, documentPublicationTypesStr, documentYearPublished);
		expectedEvidenceTableValues.add(expectedEvt);

		EntityTableValues subjectEtv = new EntityTableValues(subjectEntityId, subjectSpanStr, "Ceftriaxone");
		EntityTableValues objectEtv = new EntityTableValues(objectEntityId, objectSpanStr, "pneumonia");
		expectedEntityTableValues.add(subjectEtv);
		expectedEntityTableValues.add(objectEtv);

		EvidenceScoreTableValues falseEstv = new EvidenceScoreTableValues(evidenceId, "false", 0.0003744);
		EvidenceScoreTableValues treatsEstv = new EvidenceScoreTableValues(evidenceId,
				BiolinkPredicate.BL_TREATS.getCurie(), 0.9994267);
		EvidenceScoreTableValues contributesToEstv = new EvidenceScoreTableValues(evidenceId,
				BiolinkPredicate.BL_CONTRIBUTES_TO.getCurie(), 0.00019880748);
		expectedEvidenceScoreTableValues.add(falseEstv);
		expectedEvidenceScoreTableValues.add(treatsEstv);
		expectedEvidenceScoreTableValues.add(contributesToEstv);

		assertEquals(expectedAssertionTableValues, assertionTableValues);
		assertEquals(expectedEvidenceTableValues, evidenceTableValues);
		assertEquals(expectedEntityTableValues, entityTableValues);
		assertEquals(new HashSet<EvidenceScoreTableValues>(expectedEvidenceScoreTableValues),
				new HashSet<EvidenceScoreTableValues>(evidenceScoreTableValues));

	}

}
