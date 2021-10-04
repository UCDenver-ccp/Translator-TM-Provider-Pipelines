package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;

/**
 * Reads in data from a google sheet that was produced by
 * {@link GoogleSheetsAssertionAnnotationSheetCreator} and outputs or appends to
 * a file that will be used to train a BERT model.
 *
 */
public class GoogleSheetsToBertInputFileCreator {

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	public static void exportRowsToBertTrainingFile(File credentialsFile, File dataStoreDirectory, String spreadsheetId,
			int startRowIndex, int endRowIndex, String endColumnIndex, File outputFile, WriteMode writeMode,
			BiolinkPredicate... predicates) throws IOException, GeneralSecurityException {
		String applicationName = "extracting rows";
		DataStoreFactory dataStoreFactory = new FileDataStoreFactory(dataStoreDirectory);
		// Build a new authorized API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				GoogleSheetsAssertionAnnotationSheetCreator.getCredentials(HTTP_TRANSPORT, credentialsFile,
						dataStoreFactory)).setApplicationName(applicationName).build();

		String range = "A" + startRowIndex + ":" + endColumnIndex + endRowIndex;

		ValueRange result = sheetsService.spreadsheets().values().get(spreadsheetId, range).execute();
//		int numRows = result.getValues() != null ? result.getValues().size() : 0;
		List<List<Object>> rows = result.getValues();

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile, CharacterEncoding.UTF_8, writeMode,
				FileSuffixEnforcement.OFF)) {
			for (List<Object> row : rows) {
				String sentenceWithPlaceholder = row
						.get(GoogleSheetsAssertionAnnotationSheetCreator.SENTENCE_WITH_PLACEHOLDER_COLUMN).toString();
				BiolinkPredicate predicate = null;
				for (int i = 0; i < predicates.length; i++) {
					boolean val = Boolean.parseBoolean(
							row.get(GoogleSheetsAssertionAnnotationSheetCreator.NO_RELATION_COLUMN + i).toString());
					if (val) {
						predicate = predicates[i];
					}
				}
				String hash = DigestUtils.sha256Hex(sentenceWithPlaceholder);
				String outputLine = hash + "\t" + sentenceWithPlaceholder + "\t" + predicate.getEdgeLabelAbbreviation();

				writer.write(outputLine + "\n");
			}
		}

	}

}
