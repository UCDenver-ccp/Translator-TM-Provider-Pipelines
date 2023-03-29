package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.Sheets.Spreadsheets.BatchUpdate;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BooleanCondition;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.DataValidationRule;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Link;
import com.google.api.services.sheets.v4.model.RepeatCellRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.TextFormat;
import com.google.api.services.sheets.v4.model.TextFormatRun;
import com.google.api.services.sheets.v4.model.UpdateCellsRequest;
import com.google.api.services.sheets.v4.model.ValueRange;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractedSentence;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import lombok.Data;

/**
 * Takes as input a file that is a representative sampling of SemMedDB
 * predications and creates a set of google sheets for use in the Feb 2023
 * Translator Relay SemMedDB evaluation exercise.
 * 
 * Each google sheet has 2 columns: (1) a link to the feedback UI for a
 * sentence/predication; and (2) a checkbox to indicate that the
 * sentence/predication was reviewed
 * 
 * Borrows heavily from:
 * https://developers.google.com/sheets/api/quickstart/java
 *
 */

@Data
public class SemmedDbFebRelay2023GoogleSheetsCreator {

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static final int UI_URL_COLUMN = 0;
	private static final int CHECKBOX_COLUMN = 1;

	/**
	 * Global instance of the scopes required by this quickstart. If modifying these
	 * scopes, delete your previously saved tokens/ folder.
	 */
	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);

	private static final String BASE_TMUI_URL = "https://tmui.text-mining-kp.org/semmed/predication";

	/**
	 * Used to store Google credentials/tokens
	 */
	private final DataStoreFactory dataStoreFactory;
	private long timeIntervalStart;
	private int requestCount = 0;

	public SemmedDbFebRelay2023GoogleSheetsCreator(File tokenDirectory) throws IOException {
		dataStoreFactory = new FileDataStoreFactory(tokenDirectory);
		timeIntervalStart = System.currentTimeMillis();
	}

	/**
	 * Creates an authorized Credential object.
	 * 
	 * @param HTTP_TRANSPORT The network HTTP Transport.
	 * @return An authorized Credential object.
	 * @throws IOException If the credentials.json file cannot be found.
	 */
	public static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT, File credentialsFile,
			DataStoreFactory dataStoreFactory) throws IOException {
		// Load client secrets.
		InputStream in = new FileInputStream(credentialsFile);
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				clientSecrets, SCOPES).setDataStoreFactory(dataStoreFactory).setAccessType("offline").build();
		LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}

	/**
	 * @param credentialsFile
	 * @param predicationIds
	 * @param sheetTitle
	 * @throws IOException
	 * @throws GeneralSecurityException
	 * @throws InterruptedException
	 */
	public void createIndexSpreadsheet(File credentialsFile, List<String> sheetIds)
			throws IOException, GeneralSecurityException, InterruptedException {

		String sheetTitle = "Index sheet";
		System.out.println("Creating new spreadsheet: " + sheetTitle);

		// Build a new authorized API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				getCredentials(HTTP_TRANSPORT, credentialsFile, dataStoreFactory)).setApplicationName(sheetTitle)
						.build();

		String sheetId = createNewSheet(sheetsService, sheetTitle);

		writeHeaderToSpreadsheet(sheetsService, sheetId);
		List<Request> updateRequests = new ArrayList<Request>();

		int row = 1;
		for (String annotationSheetId : sheetIds) {
			/* write the text for the links in the first column */
			List<CellData> cellDataList = getSentenceCellData("batch_" + (row + 2));

			AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
			RowData rowData = new RowData().setValues(cellDataList);
			appendCellsRequest.setFields("UserEnteredValue");
			List<RowData> rowDataList = Arrays.asList(rowData);
			appendCellsRequest.setRows(rowDataList);

			updateRequests.add(new Request().setAppendCells(appendCellsRequest));
			updateRequests.addAll(createSheetIdHyperlinkRequests(annotationSheetId, row++));

			// cannot send more than 100,000 requests in an update, so we check to see if we
			// are near the limit and send the update request if we are.
			if (updateRequests.size() > 90000) {
				System.out.println("Sending intermediate batch of update requests.");
				BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest();
				content.setRequests(updateRequests);
				BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
				batchUpdate.execute();
				updateRequests = new ArrayList<Request>();
			}

		}

		System.out.println("Sending final batch of update requests.");
		BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest();
		content.setRequests(updateRequests);
		BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
		batchUpdate.execute();

	}

	/**
	 * @param credentialsFile
	 * @param predicationIds
	 * @param sheetTitle
	 * @throws IOException
	 * @throws GeneralSecurityException
	 * @throws InterruptedException
	 */
	public String createNewSpreadsheet(File credentialsFile, List<String> predicationIds, String sheetTitle,
			String folderId) throws IOException, GeneralSecurityException, InterruptedException {

		System.out.println("Creating new spreadsheet: " + sheetTitle);

		// Build a new authorized API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				getCredentials(HTTP_TRANSPORT, credentialsFile, dataStoreFactory)).setApplicationName(sheetTitle)
						.build();

		String sheetId = createNewSheet(sheetsService, sheetTitle);

		writeHeaderToSpreadsheet(sheetsService, sheetId);
		List<Request> updateRequests = new ArrayList<Request>();

		int row = 1;
		for (String predicationId : predicationIds) {
			/* write the text for the links in the first column */
			List<CellData> cellDataList = getSentenceCellData(predicationId);

			AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
			RowData rowData = new RowData().setValues(cellDataList);
			appendCellsRequest.setFields("UserEnteredValue");
			List<RowData> rowDataList = Arrays.asList(rowData);
			appendCellsRequest.setRows(rowDataList);

			updateRequests.add(new Request().setAppendCells(appendCellsRequest));
			updateRequests.addAll(createPredicationIdHyperlinkRequests(predicationId, row++));

			// cannot send more than 100,000 requests in an update, so we check to see if we
			// are near the limit and send the update request if we are.
			if (updateRequests.size() > 90000) {
				System.out.println("Sending intermediate batch of update requests.");
				BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest();
				content.setRequests(updateRequests);
				BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
				batchUpdate.execute();
				updateRequests = new ArrayList<Request>();
			}

		}

		System.out.println("Sending final batch of update requests.");
		BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest();
		content.setRequests(updateRequests);
		BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
		batchUpdate.execute();

		// write checkboxes on spreadsheet
		addCheckBoxesToSheet(sheetsService, sheetId, predicationIds.size() + 1);

		return sheetId;

	}

	private List<CellData> getSentenceCellData(String predicationId) {
		List<CellData> cellDataList = new ArrayList<CellData>();
		cellDataList.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(predicationId)));
		return cellDataList;
	}

	private static void addCheckBoxesToSheet(Sheets sheetsService, String sheetId, int maxRow) throws IOException {

		int sheetTabId = 0;

		Integer startRowIndex = 1; // row zero is the header
		Integer endRowIndex = maxRow;
		Integer startColumnIndex = CHECKBOX_COLUMN;
		Integer endColumnIndex = CHECKBOX_COLUMN + 1;

		GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(startRowIndex)
				.setEndRowIndex(endRowIndex).setStartColumnIndex(startColumnIndex).setEndColumnIndex(endColumnIndex);

		String fields = "dataValidation";
		BooleanCondition booleanCondition = new BooleanCondition().setType("BOOLEAN");
		DataValidationRule dataValidation = new DataValidationRule().setCondition(booleanCondition);
		CellData cellData = new CellData().setDataValidation(dataValidation);

		RepeatCellRequest repeatCellRequest = new RepeatCellRequest();
		repeatCellRequest.setRange(range);
		repeatCellRequest.setFields(fields);
		repeatCellRequest.setCell(cellData);

		Request r = new Request();
		r.setRepeatCell(repeatCellRequest);
		List<Request> requests = Arrays.asList(r);

		BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest().setRequests(requests);
		BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
		batchUpdate.execute();
	}

	private static void writeHeaderToSpreadsheet(Sheets sheetsService, String sheetId) throws IOException {

		List<Object> headerValues = new ArrayList<Object>();
		headerValues.add("Sentence Review Link");
		headerValues.add("Completed");

		List<List<Object>> values = Arrays.asList(headerValues);
		ValueRange body = new ValueRange().setValues(values);
		sheetsService.spreadsheets().values().append(sheetId, "Sheet1", body).setValueInputOption("USER_ENTERED")
				.execute();

	}

//	private Collection<Request> writeUiUrlToSpreadsheet(String predicationId, Sheets sheetsService, String sheetId)
//			throws IOException, InterruptedException {
////		List<CellData> cellDataList = getSentenceCellData(sentenceId, sentence, biolinkAssociation, swapSubjectObject);
////
////		AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
////
////		RowData rowData = new RowData().setValues(cellDataList);
////		appendCellsRequest.setFields("UserEnteredValue");
////		List<RowData> rowDataList = Arrays.asList(rowData);
////		appendCellsRequest.setRows(rowDataList);
//
//		List<Request> updateRequests = new ArrayList<Request>();
////		updateRequests.add(new Request().setAppendCells(appendCellsRequest));
////		updateRequests
////				.add(createColorColumnTextUpdateRequest(extractedSentenceCount, subjectFormat, SUBJECT_TEXT_COLUMN));
////		updateRequests
////				.add(createColorColumnTextUpdateRequest(extractedSentenceCount, objectFormat, OBJECT_TEXT_COLUMN));
////		updateRequests.add(createColorSentenceUpdateRequest(sentence, extractedSentenceCount, biolinkAssociation,
////				swapSubjectObject));
//		updateRequests.addAll(createEntityIdHyperlinkRequests(sentence, extractedSentenceCount, biolinkAssociation,
//				swapSubjectObject));
//
//		return updateRequests;
//	}

//	private List<CellData> getSentenceCellData(String sentenceId, ExtractedSentence sentence,
//			BiolinkAssociation biolinkAssociation, boolean swapSubjectObject) {
//		// -1 because there is always a triple representing no_relation
//		int relationCount = biolinkAssociation.getSpoTriples().length - 1;
//
//		List<CellData> cellDataList = new ArrayList<CellData>();
//		cellDataList.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(sentenceId)));
//		cellDataList.add(new CellData()
//				.setUserEnteredValue(new ExtendedValue().setStringValue(sentence.getSentenceWithPlaceholders())));
//		cellDataList
//				.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(sentence.getDocumentId())));
//
//		if (!swapSubjectObject) {
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getSubjectId(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getSubjectText(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getObjectId(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getObjectText(sentence, biolinkAssociation))));
//		} else {
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getObjectId(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getObjectText(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getSubjectId(sentence, biolinkAssociation))));
//			cellDataList.add(new CellData().setUserEnteredValue(
//					new ExtendedValue().setStringValue(getSubjectText(sentence, biolinkAssociation))));
//		}
//
//		cellDataList.add(new CellData().setUserEnteredValue(
//				new ExtendedValue().setStringValue(sentence.getSentenceText() + "                  ")));
//		cellDataList.add(new CellData().setUserEnteredValue(new ExtendedValue().setBoolValue(true)));
//
//		// add a column for each relation
//		for (int i = 0; i < relationCount; i++) {
//			cellDataList.add(new CellData().setUserEnteredValue(new ExtendedValue().setBoolValue(false)));
//		}
//		return cellDataList;
//	}

	private static Collection<? extends Request> createSheetIdHyperlinkRequests(String sheetId, int row) {

		int sheetTabId = 0;

		List<Request> updateRequests = new ArrayList<Request>();

		String uri = String.format("https://docs.google.com/spreadsheets/d/%s", sheetId);

		{
			TextFormat linkFormat = new TextFormat().setLink(new Link().setUri(uri));
			GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(row).setEndRowIndex(row + 1)
					.setStartColumnIndex(UI_URL_COLUMN).setEndColumnIndex(UI_URL_COLUMN + 1);

			TextFormatRun textFormatRun = new TextFormatRun();
			textFormatRun.setFormat(linkFormat);
			textFormatRun.setStartIndex(0);
			CellData cellData = new CellData().setTextFormatRuns(Arrays.asList(textFormatRun));
			List<CellData> cellDataList = Arrays.asList(cellData);
			RowData rowData = new RowData().setValues(cellDataList);
			List<RowData> rows = Arrays.asList(rowData);

			UpdateCellsRequest updateCellRequest = new UpdateCellsRequest();
			updateCellRequest.setRange(range);
			updateCellRequest.setRows(rows);
			updateCellRequest.setFields("textFormatRuns");

			Request r = new Request();
			r.setUpdateCells(updateCellRequest);
			updateRequests.add(r);
		}

		return updateRequests;

	}

	private static Collection<? extends Request> createPredicationIdHyperlinkRequests(String predicationId, int row) {

		int sheetTabId = 0;

		List<Request> updateRequests = new ArrayList<Request>();

		String uri = String.format("%s/%s", BASE_TMUI_URL, predicationId);

		{
			TextFormat linkFormat = new TextFormat().setLink(new Link().setUri(uri));
			GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(row).setEndRowIndex(row + 1)
					.setStartColumnIndex(UI_URL_COLUMN).setEndColumnIndex(UI_URL_COLUMN + 1);

			TextFormatRun textFormatRun = new TextFormatRun();
			textFormatRun.setFormat(linkFormat);
			textFormatRun.setStartIndex(0);
			CellData cellData = new CellData().setTextFormatRuns(Arrays.asList(textFormatRun));
			List<CellData> cellDataList = Arrays.asList(cellData);
			RowData rowData = new RowData().setValues(cellDataList);
			List<RowData> rows = Arrays.asList(rowData);

			UpdateCellsRequest updateCellRequest = new UpdateCellsRequest();
			updateCellRequest.setRange(range);
			updateCellRequest.setRows(rows);
			updateCellRequest.setFields("textFormatRuns");

			Request r = new Request();
			r.setUpdateCells(updateCellRequest);
			updateRequests.add(r);
		}

		return updateRequests;

	}

//	private Request createColorSentenceUpdateRequest(ExtractedSentence sentence, int extractedSentenceCount,
//			BiolinkAssociation biolinkAssociation, boolean swapSubjectObject) {
//		int sheetTabId = 0;
//
//		GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(extractedSentenceCount)
//				.setEndRowIndex(extractedSentenceCount + 1).setStartColumnIndex(SENTENCE_COLUMN)
//				.setEndColumnIndex(SENTENCE_COLUMN + 1);
//
//		Map<Span, TextFormat> formatMap = new HashMap<Span, TextFormat>();
//
//		int sentenceLength = sentence.getSentenceText().length();
//
//		List<Span> subjectSpans = getSubjectSpan(sentence, biolinkAssociation);
//		List<Span> objectSpans = getObjectSpan(sentence, biolinkAssociation);
//		if (swapSubjectObject) {
//			subjectSpans = getObjectSpan(sentence, biolinkAssociation);
//			objectSpans = getSubjectSpan(sentence, biolinkAssociation);
//		}
//
//		for (Span span : subjectSpans) {
//			if (span.getSpanStart() > sentenceLength) {
//				throw new IllegalStateException("start > sentence length: " + span.getSpanStart() + " > "
//						+ sentenceLength + " " + span.toString() + " -- " + sentence.getSentenceText());
//			}
//			formatMap.put(span, subjectFormat);
//		}
////		Collections.sort(subjectSpans, Span.ASCENDING());
//
//		for (Span span : objectSpans) {
//			if (span.getSpanStart() > sentenceLength) {
//				throw new IllegalStateException("start > sentence length: " + span.getSpanStart() + " > "
//						+ sentenceLength + " " + span.toString() + " -- " + sentence.getSentenceText());
//			}
//			formatMap.put(span, objectFormat);
//		}
////		Collections.sort(objectSpans, Span.ASCENDING());
//
//		Map<Span, TextFormat> sortedFormatMap = sortMapByKeys(formatMap, Span.ASCENDING());
//
////		List<TextFormatRun> subjectFormatRuns = getFormatRuns(subjectFormatOn, formatOff, subjectSpans);
////		List<TextFormatRun> objectFormatRuns = getFormatRuns(objectFormatOn, formatOff, objectSpans);
//
//		List<TextFormatRun> textFormatRuns = getTextFormatRuns(sortedFormatMap, defaultFormat);
////		textFormatRuns.addAll(subjectFormatRuns);
////		textFormatRuns.addAll(objectFormatRuns);
//
//		CellData cellData = new CellData().setTextFormatRuns(textFormatRuns);
//		List<CellData> cellDataList = Arrays.asList(cellData);
//		RowData rowData = new RowData().setValues(cellDataList);
//		List<RowData> rows = Arrays.asList(rowData);
//
//		UpdateCellsRequest updateCellRequest = new UpdateCellsRequest();
//		updateCellRequest.setRange(range);
//		updateCellRequest.setRows(rows);
//		updateCellRequest.setFields("textFormatRuns");
//
//		Request r = new Request();
//		r.setUpdateCells(updateCellRequest);
//
//		return r;
//	}
//
//	private Request createColorColumnTextUpdateRequest(int extractedSentenceCount, TextFormat textFormat,
//			int columnIndex) {
//		int sheetTabId = 0;
//
//		GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(extractedSentenceCount)
//				.setEndRowIndex(extractedSentenceCount + 1).setStartColumnIndex(columnIndex)
//				.setEndColumnIndex(columnIndex + 1);
//
//		TextFormatRun formatOnRun = new TextFormatRun();
//		formatOnRun.setFormat(textFormat);
//		formatOnRun.setStartIndex(0);
//		List<TextFormatRun> textFormatRuns = Arrays.asList(formatOnRun);
//
//		CellData cellData = new CellData().setTextFormatRuns(textFormatRuns);
//		List<CellData> cellDataList = Arrays.asList(cellData);
//		RowData rowData = new RowData().setValues(cellDataList);
//		List<RowData> rows = Arrays.asList(rowData);
//
//		UpdateCellsRequest updateCellRequest = new UpdateCellsRequest();
//		updateCellRequest.setRange(range);
//		updateCellRequest.setRows(rows);
//		updateCellRequest.setFields("textFormatRuns");
//
//		Request r = new Request();
//		r.setUpdateCells(updateCellRequest);
//
//		return r;
//	}
//
//	private static List<TextFormatRun> getTextFormatRuns(Map<Span, TextFormat> sortedFormatMap,
//			TextFormat defaultFormat) {
//		List<TextFormatRun> formatRuns = new ArrayList<TextFormatRun>();
//		for (Entry<Span, TextFormat> entry : sortedFormatMap.entrySet()) {
//			Span span = entry.getKey();
//			TextFormat format = entry.getValue();
//
//			TextFormatRun formatOnRun = new TextFormatRun();
//			formatOnRun.setFormat(format);
//			formatOnRun.setStartIndex(span.getSpanStart());
//			formatRuns.add(formatOnRun);
//
//			TextFormatRun formatOffRun = new TextFormatRun();
//			formatOffRun.setFormat(defaultFormat);
//			formatOffRun.setStartIndex(span.getSpanEnd());
//			formatRuns.add(formatOffRun);
//
//		}
//		return formatRuns;
//	}
//
//	private static Map<Span, TextFormat> sortMapByKeys(Map<Span, TextFormat> formatMap,
//			final Comparator<Span> comparator) {
//		ArrayList<Entry<Span, TextFormat>> entryList = new ArrayList<Entry<Span, TextFormat>>(formatMap.entrySet());
//		Collections.sort(entryList, new Comparator<Entry<Span, TextFormat>>() {
//
//			@Override
//			public int compare(Entry<Span, TextFormat> entry1, Entry<Span, TextFormat> entry2) {
//				return comparator.compare(entry1.getKey(), entry2.getKey());
//			}
//
//		});
//		Map<Span, TextFormat> sortedMap = new LinkedHashMap<Span, TextFormat>();
//		for (Entry<Span, TextFormat> entry : entryList) {
//			sortedMap.put(entry.getKey(), entry.getValue());
//		}
//		return sortedMap;
//
//	}

//	public static <K extends Comparable<K>, V> Map<K, V> sortMapByKeys(Map<K, V> inputMap, final SortOrder sortOrder, Comparator comparator) {
//		ArrayList<Entry<K, V>> entryList = new ArrayList<Entry<K, V>>(inputMap.entrySet());
//		Collections.sort(entryList, new Comparator<Entry<K, V>>() {
//
//			@Override
//			public int compare(Entry<K, V> entry1, Entry<K, V> entry2) {
//				return entry1.getKey().compareTo(entry2.getKey()) * sortOrder.modifier();
//			}
//
//		});
//		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
//		for (Entry<K, V> entry : entryList) {
//			sortedMap.put(entry.getKey(), entry.getValue());
//		}
//		return sortedMap;
//	}

//	private static List<TextFormatRun> getFormatRuns(TextFormat formatOn, TextFormat formatOff, List<Span> spans) {
//
//		List<TextFormatRun> formatRuns = new ArrayList<TextFormatRun>();
//		for (Span span : spans) {
//			TextFormatRun formatOnRun = new TextFormatRun();
//			formatOnRun.setFormat(formatOn);
//			formatOnRun.setStartIndex(span.getSpanStart());
//			formatRuns.add(formatOnRun);
//
//			TextFormatRun formatOffRun = new TextFormatRun();
//			formatOffRun.setFormat(formatOff);
//			formatOffRun.setStartIndex(span.getSpanEnd());
//			formatRuns.add(formatOffRun);
//		}
//
//		return formatRuns;
//
//	}

//	private String getSubjectText(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String subjectPlaceholder = biolinkAssociation.getSubjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
//			return sentence.getEntityCoveredText1();
//		}
//		return sentence.getEntityCoveredText2();
//	}
//
//	private String getObjectText(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String objectPlaceholder = biolinkAssociation.getObjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
//			return sentence.getEntityCoveredText1();
//		}
//		return sentence.getEntityCoveredText2();
//	}
//
//	private static List<Span> getSubjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String subjectPlaceholder = biolinkAssociation.getSubjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
//			return sentence.getEntitySpan1();
//		}
//		return sentence.getEntitySpan2();
//	}
//
//	private static List<Span> getObjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String objectPlaceholder = biolinkAssociation.getObjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
//			return sentence.getEntitySpan1();
//		}
//		return sentence.getEntitySpan2();
//	}
//
//	private static String getSubjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String subjectPlaceholder = biolinkAssociation.getSubjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
//			return sentence.getEntityId1();
//		}
//		return sentence.getEntityId2();
//	}
//
//	private static String getObjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
//		String objectPlaceholder = biolinkAssociation.getObjectClass().getPlaceholder();
//		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
//			return sentence.getEntityId1();
//		}
//		return sentence.getEntityId2();
//	}

	private static String createNewSheet(Sheets sheetsService, String sheetTitle) throws IOException {
		Spreadsheet spreadSheet = new Spreadsheet().setProperties(new SpreadsheetProperties().setTitle(sheetTitle));
		Spreadsheet result = sheetsService.spreadsheets().create(spreadSheet).execute();
		return result.getSpreadsheetId();
	}

	protected static String computeHash(ExtractedSentence sentence) {
		return DigestUtil.getBase64Sha1Digest(sentence.getSentenceText());
	}

//	/**
//	 * @param inputSentenceFile
//	 * @return the number of sentences in the specified file -- this is not just the
//	 *         line count, but the count of unique sentences. Many sentences appear
//	 *         multiple times (on multiple lines) because the contain multiple
//	 *         entities. This method does assume that the different entries for each
//	 *         particular sentence appear on consecutive lines in the input file.
//	 */
//	protected static int countSentences(List<File> inputSentenceFiles) throws IOException {
//		int sentenceCount = 0;
//		String previousSentenceText = null;
//
//		for (File inputSentenceFile : inputSentenceFiles) {
//			try (InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
//					? new GZIPInputStream(new FileInputStream(inputSentenceFile))
//					: new FileInputStream(inputSentenceFile)) {
//
//				for (StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null); lineIter.hasNext();) {
//					Line line = lineIter.next();
//					String[] cols = line.getText().split("\\t");
//					String sentenceText = cols[10];
//					if (previousSentenceText == null || !previousSentenceText.equals(sentenceText)) {
//						sentenceCount++;
//						previousSentenceText = sentenceText;
//					}
//				}
//			}
//		}
//		return sentenceCount;
//	}
//
//	protected static Set<Integer> getRandomIndexes(int maxSentenceCount, int batchSize) {
//		Set<Integer> randomIndexes = new HashSet<Integer>();
//
//		// add 100 extra just in case there are collisions with previous extracted
//		// sentences
//		Random rand = new Random();
//		while (randomIndexes.size() < maxSentenceCount && randomIndexes.size() < batchSize + 10000) {
//			randomIndexes.add(rand.nextInt(maxSentenceCount) + 1);
//
//		}
//
//		return randomIndexes;
//	}

}
