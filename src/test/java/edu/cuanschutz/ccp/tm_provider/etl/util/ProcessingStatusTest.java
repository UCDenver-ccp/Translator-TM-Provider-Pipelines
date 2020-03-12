package edu.cuanschutz.ccp.tm_provider.etl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;

public class ProcessingStatusTest {

	@Test
	public void test() {
		String documentId = "PMC12345";
		ProcessingStatus status = new ProcessingStatus(documentId, EnumSet.of(ProcessingStatusFlag.TEXT_DONE));

		assertEquals(documentId, status.getDocumentId());
		assertTrue(status.isTextDone());
		assertFalse(status.isBertChebiDone());

		// test with no flags set
		status = new ProcessingStatus(documentId, EnumSet.noneOf(ProcessingStatusFlag.class));
		assertEquals(documentId, status.getDocumentId());
		assertFalse(status.isTextDone());
		assertFalse(status.isBertChebiDone());

		// test with all flags set
		status = new ProcessingStatus(documentId, EnumSet.allOf(ProcessingStatusFlag.class));
		assertEquals(documentId, status.getDocumentId());
		assertTrue(status.isTextDone());
		assertTrue(status.isDependencyParseDone());
		// OGER flags
		assertTrue(status.isOgerChebiDone());
		assertTrue(status.isOgerClDone());
		assertTrue(status.isOgerGoBpDone());
		assertTrue(status.isOgerGoCcDone());
		assertTrue(status.isOgerGoMfDone());
		assertTrue(status.isOgerMopDone());
		assertTrue(status.isOgerNcbiTaxonDone());
		assertTrue(status.isOgerPrDone());
		assertTrue(status.isOgerSoDone());
		assertTrue(status.isOgerUberonDone());
		// BERT flags
		assertTrue(status.isBertChebiDone());
		assertTrue(status.isBertClDone());
		assertTrue(status.isBertGoBpDone());
		assertTrue(status.isBertGoCcDone());
		assertTrue(status.isBertGoMfDone());
		assertTrue(status.isBertMopDone());
		assertTrue(status.isBertNcbiTaxonDone());
		assertTrue(status.isBertPrDone());
		assertTrue(status.isBertSoDone());
		assertTrue(status.isBertUberonDone());

	}

}
