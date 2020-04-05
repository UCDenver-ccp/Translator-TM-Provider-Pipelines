package edu.cuanschutz.ccp.tm_provider.etl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;

public class ProcessingStatusTest {

	@Test
	public void test() {
		String documentId = "PMC12345";
		ProcessingStatus status = new ProcessingStatus(documentId);
		DocumentCriteria dc = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT,
				"0.1.0");
		status.enableFlag(ProcessingStatusFlag.TEXT_DONE, dc, 1);

		assertEquals(documentId, status.getDocumentId());
		assertTrue(status.getFlagPropertyValue(ProcessingStatusFlag.TEXT_DONE.getDatastoreFlagPropertyName()));
		assertFalse(
				status.getFlagPropertyValue(ProcessingStatusFlag.BERT_CHEBI_IDS_DONE.getDatastoreFlagPropertyName()));

		// test with no flags set
		status = new ProcessingStatus(documentId);
		assertEquals(documentId, status.getDocumentId());
		assertFalse(status.getFlagPropertyValue(ProcessingStatusFlag.TEXT_DONE.getDatastoreFlagPropertyName()));
		assertFalse(
				status.getFlagPropertyValue(ProcessingStatusFlag.BERT_CHEBI_IDS_DONE.getDatastoreFlagPropertyName()));
	}

}
