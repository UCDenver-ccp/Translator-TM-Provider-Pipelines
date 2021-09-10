package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPInputStream;

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
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BooleanCondition;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.Color;
import com.google.api.services.sheets.v4.model.DataValidationRule;
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
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.SPO;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import lombok.Data;

/**
 * Takes as input a file containing sentences pulled using the
 * SentenceExtractionPipeline which produces a TSV file containing metadata that
 * accompanies each sentence/assertion.
 *
 */
/**
 * @author bill
 *
 */
@Data
public class GoogleSheetsAssertionAnnotationSheetCreator {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static final String TOKENS_DIRECTORY_PATH = "tokens";

	private static final int SENTENCE_ID_COLUMN = 0;
	private static final int DOCUMENT_ID_COLUMN = 1;
	private static final int SUBJECT_ID_COLUMN = 2;
	private static final int OBJECT_ID_COLUMN = 3;
	private static final int SENTENCE_COLUMN = 4;
	private static final int NO_RELATION_COLUMN = 5;

	/**
	 * Global instance of the scopes required by this quickstart. If modifying these
	 * scopes, delete your previously saved tokens/ folder.
	 */
	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);

	/**
	 * Used to store Google credentials/tokens
	 */
	private final DataStoreFactory dataStoreFactory;

	public GoogleSheetsAssertionAnnotationSheetCreator(File dataStoreDirectory) throws IOException {
		dataStoreFactory = new FileDataStoreFactory(dataStoreDirectory);
	}

	/**
	 * Creates an authorized Credential object.
	 * 
	 * @param HTTP_TRANSPORT The network HTTP Transport.
	 * @return An authorized Credential object.
	 * @throws IOException If the credentials.json file cannot be found.
	 */
	private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT, File credentialsFile) throws IOException {
		// Load client secrets.
		InputStream in = new FileInputStream(credentialsFile);
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				clientSecrets, SCOPES).setDataStoreFactory(dataStoreFactory)
//						.setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
						.setAccessType("offline").build();
		LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}

	/**
	 * @param credentialsFile
	 * @param biolinkAssociation
	 * @param batchId             identifier for this batch - will be appended to
	 *                            the sheet title (and therefore sheet file name) to
	 *                            make it unique
	 * @param batchSize
	 * @param inputSentenceFile
	 * @param outputSpreadsheetId
	 * @param previousSubsetFiles
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public void createNewSpreadsheet(File credentialsFile, BiolinkAssociation biolinkAssociation, String batchId,
			int batchSize, File inputSentenceFile, File previousSentenceIdsFile)
			throws IOException, GeneralSecurityException {

		String sheetTitle = biolinkAssociation.name() + "-" + batchId;
		String applicationName = "annotation of " + biolinkAssociation.name();

		// Build a new authorized API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				getCredentials(HTTP_TRANSPORT, credentialsFile)).setApplicationName(applicationName).build();

		String sheetId = createNewSheet(sheetsService, sheetTitle);

		Set<String> alreadyAnnotated = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(previousSentenceIdsFile, UTF8));

		int maxSentenceCount = countSentences(inputSentenceFile);

		Set<Integer> indexesForNewBatch = getRandomIndexes(maxSentenceCount, batchSize);

		List<Request> updateRequests = new ArrayList<Request>();
		updateRequests.addAll(writeHeaderToSpreadsheet(biolinkAssociation, sheetsService, sheetId));

		Set<String> hashesOutputInThisBatch = new HashSet<String>();
		// this count is used to track what line a sentence ends up in the Google Sheet
		int extractedSentenceCount = 1;
		int sentenceCount = 0;
		String previousSentenceText = null;
		InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
				? new GZIPInputStream(new FileInputStream(inputSentenceFile))
				: new FileInputStream(inputSentenceFile);
		StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null);
		try {
			while (lineIter.hasNext() && hashesOutputInThisBatch.size() < batchSize) {
				Line line = lineIter.next();
				ExtractedSentence sentence = ExtractedSentence.fromTsv(line.getText(), true);
				if (previousSentenceText == null || !previousSentenceText.equals(sentence.getSentenceText())) {
					previousSentenceText = sentence.getSentenceText();
					sentenceCount++;
				}

				if (indexesForNewBatch.contains(sentenceCount)) {
					String hash = computeHash(sentence);
					if (!alreadyAnnotated.contains(hash)) {
						hashesOutputInThisBatch.add(hash);
						updateRequests.addAll(writeSentenceToSpreadsheet(hash, sentence, sheetsService, sheetId,
								extractedSentenceCount, biolinkAssociation));
						extractedSentenceCount++;
					}
				}
			}
		} finally {
			lineIter.close();
		}

		/*
		 * save the hashes for sentences that were output during this batch to the file
		 * that tracks sentence hashes that have already been exported to a sheet for
		 * annotation
		 */
		try (BufferedWriter alreadyAnnotatedWriter = FileWriterUtil.initBufferedWriter(previousSentenceIdsFile, UTF8,
				WriteMode.APPEND, FileSuffixEnforcement.OFF)) {
			for (String hash : hashesOutputInThisBatch) {
				alreadyAnnotatedWriter.write(hash + "\n");
			}
		}

		// perform updates (formatting) on sentences
		BatchUpdateSpreadsheetRequest content = new BatchUpdateSpreadsheetRequest();
		content.setRequests(updateRequests);
		BatchUpdate batchUpdate = sheetsService.spreadsheets().batchUpdate(sheetId, content);
		batchUpdate.execute();

		// write checkboxes on spreadsheet
		addCheckBoxesToSheet(sheetsService, sheetId, extractedSentenceCount, biolinkAssociation.getSpoTriples().length);

	}

	private static void addCheckBoxesToSheet(Sheets sheetsService, String sheetId, int maxRow, int predicateCount)
			throws IOException {

		int sheetTabId = 0;

		Integer startRowIndex = 1; // row zero is the header
		Integer endRowIndex = maxRow;
		Integer startColumnIndex = NO_RELATION_COLUMN;
		Integer endColumnIndex = NO_RELATION_COLUMN + predicateCount;

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

	private static Collection<Request> writeHeaderToSpreadsheet(BiolinkAssociation association, Sheets sheetsService,
			String sheetId) throws IOException {

		List<Object> headerValues = new ArrayList<Object>();
		headerValues.add("Sentence ID");
		headerValues.add("Document ID");
		headerValues.add(association.getSubjectPlaceholder());
		headerValues.add(association.getObjectPlaceholder());
		headerValues.add("Sentence");
		headerValues.add("NO RELATION PRESENT");

		for (SPO spo : association.getSpoTriples()) {
			if (spo.getPredicate() != BiolinkPredicate.NO_RELATION_PRESENT) {
				headerValues.add(spo.getPredicate().name());
			}
		}

		List<List<Object>> values = Arrays.asList(headerValues);
		ValueRange body = new ValueRange().setValues(values);
		sheetsService.spreadsheets().values().append(sheetId, "Sheet1", body).setValueInputOption("USER_ENTERED")
				.execute();

		List<Request> updateRequests = new ArrayList<Request>();

		// TODO: make the header row bold
//		updateRequests.add();

		return updateRequests;
	}

	private static Collection<Request> writeSentenceToSpreadsheet(String sentenceId, ExtractedSentence sentence,
			Sheets sheetsService, String sheetId, int extractedSentenceCount, BiolinkAssociation biolinkAssociation)
			throws IOException {
		List<List<Object>> values = Arrays
				.asList(Arrays.asList(sentenceId, sentence.getDocumentId(), getSubjectId(sentence, biolinkAssociation),
						getObjectId(sentence, biolinkAssociation), sentence.getSentenceText(), true, false, false));
		ValueRange body = new ValueRange().setValues(values);

		// USER_ENTERED could also be RAW: see
		// https://developers.google.com/sheets/api/guides/values
		sheetsService.spreadsheets().values().append(sheetId, "Sheet1", body).setValueInputOption("USER_ENTERED")
				.execute();

		List<Request> updateRequests = new ArrayList<Request>();

		updateRequests.add(createColorSentenceUpdateRequest(sentence, extractedSentenceCount, biolinkAssociation));
		updateRequests.addAll(createEntityIdHyperlinkRequests(sentence, extractedSentenceCount, biolinkAssociation));

		return updateRequests;
	}

	private static Collection<? extends Request> createEntityIdHyperlinkRequests(ExtractedSentence sentence,
			int extractedSentenceCount, BiolinkAssociation biolinkAssociation) {

		int sheetTabId = 0;
		int startRowIndex = extractedSentenceCount;
		int endRowIndex = extractedSentenceCount + 1;

		List<Request> updateRequests = new ArrayList<Request>();

		String docId = sentence.getDocumentId();
		if (docId.startsWith("PMID:")) {
			docId = docId.substring(5);
		}

		String documentIdUri = "https://pubmed.ncbi.nlm.nih.gov/" + docId;
		String subjectIdUri = "http://purl.obolibrary.org/obo/"
				+ getSubjectId(sentence, biolinkAssociation).replace(":", "_");
		String objectIdUri = "http://purl.obolibrary.org/obo/"
				+ getObjectId(sentence, biolinkAssociation).replace(":", "_");

		// document Id link
		{
			TextFormat linkFormat = new TextFormat().setLink(new Link().setUri(documentIdUri));
			GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(startRowIndex)
					.setEndRowIndex(endRowIndex).setStartColumnIndex(DOCUMENT_ID_COLUMN)
					.setEndColumnIndex(DOCUMENT_ID_COLUMN + 1);
//			CellData cellData = new CellData().setUserEnteredValue(new ExtendedValue()
//			        .setFormulaValue("=HYPERLINK(\"http://stackoverflow.com\",\"SO label\")")
//					);

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

		// subject Id link
		{
			TextFormat linkFormat = new TextFormat().setLink(new Link().setUri(subjectIdUri));
			GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(startRowIndex)
					.setEndRowIndex(endRowIndex).setStartColumnIndex(SUBJECT_ID_COLUMN)
					.setEndColumnIndex(SUBJECT_ID_COLUMN + 1);
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

		// object Id link
		{
			TextFormat linkFormat = new TextFormat().setLink(new Link().setUri(objectIdUri));
			GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(startRowIndex)
					.setEndRowIndex(endRowIndex).setStartColumnIndex(OBJECT_ID_COLUMN)
					.setEndColumnIndex(OBJECT_ID_COLUMN + 1);
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

	private static Request createColorSentenceUpdateRequest(ExtractedSentence sentence, int extractedSentenceCount,
			BiolinkAssociation biolinkAssociation) {
		int sheetTabId = 0;

		Color red = new Color();
		red.setRed(1.0f);
		red.setBlue(0.0f);
		red.setGreen(0.0f);

		Color blue = new Color();
		blue.setRed(0.0f);
		blue.setBlue(1.0f);
		blue.setGreen(0.0f);

		Color black = new Color();
		black.setRed(0.0f);
		black.setBlue(0.0f);
		black.setGreen(0.0f);

		TextFormat subjectFormat = new TextFormat();
		subjectFormat.setItalic(true);
		subjectFormat.setForegroundColor(red);

		TextFormat objectFormat = new TextFormat();
		objectFormat.setItalic(true);
		objectFormat.setForegroundColor(blue);

		TextFormat defaultFormat = new TextFormat();
		defaultFormat.setForegroundColor(black);
		defaultFormat.setItalic(false);

		GridRange range = new GridRange().setSheetId(sheetTabId).setStartRowIndex(extractedSentenceCount)
				.setEndRowIndex(extractedSentenceCount + 1).setStartColumnIndex(SENTENCE_COLUMN)
				.setEndColumnIndex(SENTENCE_COLUMN + 1);

		Map<Span, TextFormat> formatMap = new HashMap<Span, TextFormat>();

		List<Span> subjectSpans = getSubjectSpan(sentence, biolinkAssociation);
		for (Span span : subjectSpans) {
			formatMap.put(span, subjectFormat);
		}
//		Collections.sort(subjectSpans, Span.ASCENDING());

		List<Span> objectSpans = getObjectSpan(sentence, biolinkAssociation);
		for (Span span : objectSpans) {
			formatMap.put(span, objectFormat);
		}
//		Collections.sort(objectSpans, Span.ASCENDING());

		Map<Span, TextFormat> sortedFormatMap = sortMapByKeys(formatMap, Span.ASCENDING());

//		List<TextFormatRun> subjectFormatRuns = getFormatRuns(subjectFormatOn, formatOff, subjectSpans);
//		List<TextFormatRun> objectFormatRuns = getFormatRuns(objectFormatOn, formatOff, objectSpans);

		List<TextFormatRun> textFormatRuns = getTextFormatRuns(sortedFormatMap, defaultFormat);
//		textFormatRuns.addAll(subjectFormatRuns);
//		textFormatRuns.addAll(objectFormatRuns);

		CellData cellData = new CellData().setTextFormatRuns(textFormatRuns);
		List<CellData> cellDataList = Arrays.asList(cellData);
		RowData rowData = new RowData().setValues(cellDataList);
		List<RowData> rows = Arrays.asList(rowData);

		UpdateCellsRequest updateCellRequest = new UpdateCellsRequest();
		updateCellRequest.setRange(range);
		updateCellRequest.setRows(rows);
		updateCellRequest.setFields("textFormatRuns");

		Request r = new Request();
		r.setUpdateCells(updateCellRequest);

		return r;
	}

	private static List<TextFormatRun> getTextFormatRuns(Map<Span, TextFormat> sortedFormatMap,
			TextFormat defaultFormat) {
		List<TextFormatRun> formatRuns = new ArrayList<TextFormatRun>();
		for (Entry<Span, TextFormat> entry : sortedFormatMap.entrySet()) {
			Span span = entry.getKey();
			TextFormat format = entry.getValue();

			TextFormatRun formatOnRun = new TextFormatRun();
			formatOnRun.setFormat(format);
			formatOnRun.setStartIndex(span.getSpanStart());
			formatRuns.add(formatOnRun);

			TextFormatRun formatOffRun = new TextFormatRun();
			formatOffRun.setFormat(defaultFormat);
			formatOffRun.setStartIndex(span.getSpanEnd());
			formatRuns.add(formatOffRun);

		}
		return formatRuns;
	}

	private static Map<Span, TextFormat> sortMapByKeys(Map<Span, TextFormat> formatMap,
			final Comparator<Span> comparator) {
		ArrayList<Entry<Span, TextFormat>> entryList = new ArrayList<Entry<Span, TextFormat>>(formatMap.entrySet());
		Collections.sort(entryList, new Comparator<Entry<Span, TextFormat>>() {

			@Override
			public int compare(Entry<Span, TextFormat> entry1, Entry<Span, TextFormat> entry2) {
				return comparator.compare(entry1.getKey(), entry2.getKey());
			}

		});
		Map<Span, TextFormat> sortedMap = new LinkedHashMap<Span, TextFormat>();
		for (Entry<Span, TextFormat> entry : entryList) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;

	}

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

	private static List<Span> getSubjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String subjectPlaceholder = biolinkAssociation.getSubjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
			return sentence.getEntitySpan1();
		}
		return sentence.getEntitySpan2();
	}

	private static List<Span> getObjectSpan(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String objectPlaceholder = biolinkAssociation.getObjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
			return sentence.getEntitySpan1();
		}
		return sentence.getEntitySpan2();
	}

	private static String getSubjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String subjectPlaceholder = biolinkAssociation.getSubjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(subjectPlaceholder)) {
			return sentence.getEntityId1();
		}
		return sentence.getEntityId2();
	}

	private static String getObjectId(ExtractedSentence sentence, BiolinkAssociation biolinkAssociation) {
		String objectPlaceholder = biolinkAssociation.getObjectPlaceholder();
		if (sentence.getEntityPlaceholder1().equals(objectPlaceholder)) {
			return sentence.getEntityId1();
		}
		return sentence.getEntityId2();
	}

	private static String createNewSheet(Sheets sheetsService, String sheetTitle) throws IOException {
		Spreadsheet spreadSheet = new Spreadsheet().setProperties(new SpreadsheetProperties().setTitle(sheetTitle));
		Spreadsheet result = sheetsService.spreadsheets().create(spreadSheet).execute();
		return result.getSpreadsheetId();
	}

	protected static String computeHash(ExtractedSentence sentence) {
		return DigestUtil.getBase64Sha1Digest(sentence.getSentenceText());
	}

	/**
	 * @param inputSentenceFile
	 * @return the number of sentences in the specified file -- this is not just the
	 *         line count, but the count of unique sentences. Many sentences appear
	 *         multiple times (on multiple lines) because the contain multiple
	 *         entities. This method does assume that the different entries for each
	 *         particular sentence appear on consecutive lines in the input file.
	 */
	protected static int countSentences(File inputSentenceFile) throws IOException {
		int sentenceCount = 0;
		String previousSentenceText = null;
		InputStream is = (inputSentenceFile.getName().endsWith(".gz"))
				? new GZIPInputStream(new FileInputStream(inputSentenceFile))
				: new FileInputStream(inputSentenceFile);

		for (StreamLineIterator lineIter = new StreamLineIterator(is, UTF8, null); lineIter.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String sentenceText = cols[10];
			if (previousSentenceText == null || !previousSentenceText.equals(sentenceText)) {
				sentenceCount++;
				previousSentenceText = sentenceText;
			}
		}
		return sentenceCount;
	}

	protected static Set<Integer> getRandomIndexes(int maxSentenceCount, int batchSize) {
		Set<Integer> randomIndexes = new HashSet<Integer>();

		// add 100 extra just in case there are collisions with previous extracted
		// sentences
		Random rand = new Random();
		while (randomIndexes.size() < batchSize + 100) {
			randomIndexes.add(rand.nextInt(maxSentenceCount) + 1);
		}

		return randomIndexes;
	}

}
