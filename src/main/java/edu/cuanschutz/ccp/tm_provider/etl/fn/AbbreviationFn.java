package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.subprocess.kernel.SubProcessCommandLineArgs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.uima.pear.util.StringUtil;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.subprocess_pipeline.kernel.SubProcessKernel;
import edu.cuanschutz.ccp.tm_provider.etl.subprocess_pipeline.utils.CallingSubProcessUtils;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationUtil;

public class AbbreviationFn
		extends DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>> {

//	private static final Logger LOG = LoggerFactory.getLogger(AbbreviationFn.class);

	private static final long serialVersionUID = 1L;

	public static final String SHORT_FORM_TYPE = "short_form";
	public static final String LONG_FORM_TYPE = "long_form";
	public static final String HAS_SHORT_FORM_RELATION = "has_short_form";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> ABBREVIATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	private SubProcessConfiguration configuration;
	private List<String> filesToDownloadToWorker;
	private String binaryName;
	private DocumentCriteria outputDocCriteria;
	private com.google.cloud.Timestamp timestamp;

	/**
	 * @param configuration
	 * @param binaryName              the name of the compiled binary file to run
	 *                                the external process
	 * @param filesToDownloadToWorker a list of all files that need to be downloaded
	 *                                to the worker in order for the compiled binary
	 *                                to function properly
	 * @param outputDocCriteria
	 * @param timestamp
	 */
	public AbbreviationFn(SubProcessConfiguration configuration, String binaryName,
			List<String> filesToDownloadToWorker, DocumentCriteria outputDocCriteria,
			com.google.cloud.Timestamp timestamp) {
		this.configuration = configuration;
		this.binaryName = binaryName;
		this.filesToDownloadToWorker = filesToDownloadToWorker;
		this.outputDocCriteria = outputDocCriteria;
		this.timestamp = timestamp;
	}

	@Setup
	public void setUp() throws Exception {
		CallingSubProcessUtils.setUp(configuration, filesToDownloadToWorker);
	}

	@ProcessElement
	public void processElement(@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
			MultiOutputReceiver out) throws Exception {

		ProcessingStatus processingStatus = statusEntityToText.getKey();
		String docId = processingStatus.getDocumentId();

		String documentText = getDocumentText(statusEntityToText.getValue());
		String sentenceAnnotsInBioNLP = getSentenceAnnots(statusEntityToText.getValue());

		try {

			// write sentences to temporary file - one per line
			File f = File.createTempFile(UUID.randomUUID().toString(), ".bionlp");
			BioNLPDocumentReader reader = new BioNLPDocumentReader();
			TextDocument td = reader.readDocument(docId, "source",
					new ByteArrayInputStream(sentenceAnnotsInBioNLP.getBytes()),
					new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);
			Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
			try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(f)) {
				for (TextAnnotation annot : td.getAnnotations()) {
					writer.write(annot.getCoveredText() + "\n");
					sentenceToSpanMap.put(annot.getCoveredText(), annot.getAggregateSpan());
//					LOG.warn("SENTENCE-TO-FILE: " + annot.getCoveredText());
				}
			}

			SubProcessCommandLineArgs commands = new SubProcessCommandLineArgs();
			commands.addCommand(0, f.getAbsolutePath());
//			Command command = new Command(0, f.getAbsolutePath());
//			commands.putCommand(command);
			SubProcessKernel kernel = new SubProcessKernel(configuration, binaryName,
					new File(configuration.getWorkerPath()));
			List<String> results = kernel.exec(commands);

			String abbreviationsInBionlp = serializeAbbreviations(results, sentenceToSpanMap, documentText);

			if (abbreviationsInBionlp != null) {
				List<String> chunkedAbbreviationsOutput = PipelineMain.chunkContent(abbreviationsInBionlp);
				out.get(ABBREVIATIONS_TAG).output(KV.of(processingStatus, chunkedAbbreviationsOutput));
			}

		} catch (Throwable t) {
			EtlFailureData failure = new EtlFailureData(outputDocCriteria, "Failure during abbreviation detection.",
					docId, t, timestamp);
			out.get(ETL_FAILURE_TAG).output(failure);
		}

	}

	private String getSentenceAnnots(Map<DocumentCriteria, String> map) {
		for (Entry<DocumentCriteria, String> entry : map.entrySet()) {
			if (entry.getKey().getDocumentType() == DocumentType.SENTENCE) {
				return entry.getValue();
			}
		}
		throw new IllegalArgumentException(
				"No sentences found for document. Cannot proceed with abbreviation detection.");
	}

	private String getDocumentText(Map<DocumentCriteria, String> map) {
		for (Entry<DocumentCriteria, String> entry : map.entrySet()) {
			if (entry.getKey().getDocumentType() == DocumentType.TEXT) {
				return entry.getValue();
			}
		}
		throw new IllegalArgumentException("No text found for document. Cannot proceed with abbreviation detection.");
	}

	/**
	 * Serialize abbreviations as long_form and short_form entity annotations
	 * connected via the has_short_form relation, i.e., <br>
	 * long_form has_short_form short_form.
	 * 
	 * @param results
	 * @param sentenceToSpanMap
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String serializeAbbreviations(List<String> results, Map<String, Span> sentenceToSpanMap,
			String documentText) throws IOException {
		TextDocument td = new TextDocument("id", "source", documentText);
		String currentSentence = null;
		for (String line : results) {
			if (lineIsAbbreviation(line)) {
				String[] cols = line.split("\\|");
				String shortForm = cols[0].trim();
				String longForm = cols[1].trim();

				@SuppressWarnings("unused")
				String confidence = cols[2].trim();

				// TODO: we are seeing some errors where we get a NPE on the next command.
				// Somehow currentSentence is null (I think), e.g. PMID:36116712
				// use current sentence to find span offset
				int offset = sentenceToSpanMap.get(currentSentence).getSpanStart();

				// look for the long and short forms in the sentence. If they appear more than
				// once then use the pair that is closest together.
				List<Span> shortFormSpans = new ArrayList<Span>();
				Pattern p = Pattern.compile(StringUtil.toRegExpString(shortForm));
				Matcher m = p.matcher(currentSentence);
				while (m.find()) {
					shortFormSpans.add(new Span(m.start(), m.end()));
				}

//				if (shortFormSpans.isEmpty()) {
//					throw new IllegalStateException(
//							String.format("Unable to find short form (%s) in sentence: ", shortForm, currentSentence));
//				}

				List<Span> longFormSpans = new ArrayList<Span>();
				p = Pattern.compile(StringUtil.toRegExpString(longForm));
				m = p.matcher(currentSentence);
				while (m.find()) {
					longFormSpans.add(new Span(m.start(), m.end()));
				}

//				if (longFormSpans.isEmpty()) {
//					throw new IllegalStateException(
//							String.format("Unable to find long form (%s) in sentence: ", longForm, currentSentence));
//				}

				/*
				 * TODO: sometimes we aren't able to find the short/long form for an
				 * abbreviation pairing; for now we simply ignore and move on.
				 */
				if (!shortFormSpans.isEmpty() && !longFormSpans.isEmpty()) {

					Span[] shortLongFormSpans = findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
					// will be null if the only short form spans overlap with long form spans
					if (shortLongFormSpans != null) {
						TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
						Span shortFormSpan = shortLongFormSpans[0];
						Span longFormSpan = shortLongFormSpans[1];
						TextAnnotation shortFormAnnot = factory.createAnnotation(shortFormSpan.getSpanStart() + offset,
								shortFormSpan.getSpanEnd() + offset, shortForm, SHORT_FORM_TYPE);
						TextAnnotation longFormAnnot = factory.createAnnotation(longFormSpan.getSpanStart() + offset,
								longFormSpan.getSpanEnd() + offset, longForm, LONG_FORM_TYPE);

						TextAnnotationUtil.addSlotValue(longFormAnnot, HAS_SHORT_FORM_RELATION,
								shortFormAnnot.getClassMention());

						td.addAnnotation(shortFormAnnot);
						td.addAnnotation(longFormAnnot);
					}
				}

			} else {
				// line is a sentence
				currentSentence = line;
			}
		}

		String bionlp = "";
		if (td.getAnnotations() != null && !td.getAnnotations().isEmpty()) {
			BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			writer.serialize(td, outputStream, CharacterEncoding.UTF_8);
			bionlp = outputStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());
		}
		return bionlp;
	}

	/**
	 * Given at least one short form and one long form span -- return the pair of
	 * short/long form spans that appear nearest to each other in the sentence. We
	 * assume that those that appear nearest are the pair that are related. This is
	 * only necessary if the short or long form appears multiple times in the
	 * sentence, and is required because the spans are not part of the output from
	 * the Ab3P system.
	 * 
	 * @param shortFormSpans
	 * @param longFormSpans
	 * @return
	 */
	@VisibleForTesting
	protected static Span[] findNearestShortLongFormSpans(List<Span> shortFormSpans, List<Span> longFormSpans) {

		// remove any short form spans that overlap with a long form span
		Set<Span> toRemove = new HashSet<Span>();
		for (Span sfSpan : shortFormSpans) {
			for (Span lfSpan : longFormSpans) {
				if (sfSpan.overlaps(lfSpan)) {
					toRemove.add(sfSpan);
				}
			}
		}
		List<Span> updatedSfSpans = new ArrayList<Span>(shortFormSpans);
		updatedSfSpans.removeAll(toRemove);

		if (!updatedSfSpans.isEmpty()) {

			// if there are only one short and long form span, then there is no need to
			// compute distance between spans
			if (updatedSfSpans.size() == 1 && longFormSpans.size() == 1) {
				return new Span[] { updatedSfSpans.get(0), longFormSpans.get(0) };
			}

			Map<Integer, Span[]> distanceToSpansMap = new HashMap<Integer, Span[]>();

			for (Span shortFormSpan : updatedSfSpans) {
				for (Span longFormSpan : longFormSpans) {
					List<Span> spans = Arrays.asList(shortFormSpan, longFormSpan);
					Collections.sort(spans, Span.ASCENDING());
					int distance = spans.get(1).getSpanStart() - spans.get(0).getSpanEnd();
					distanceToSpansMap.put(distance, new Span[] { shortFormSpan, longFormSpan });
				}
			}

			Map<Integer, Span[]> sortedMap = CollectionsUtil.sortMapByKeys(distanceToSpansMap, SortOrder.ASCENDING);
			return sortedMap.entrySet().iterator().next().getValue();
		}
		return null;
	}

	/**
	 * abbreviation lines have 3 columns, <br>
	 * e.g. ALDH1A3|aldehyde dehydrogenase 1A3|0.999613
	 * 
	 * Unfortunately there are also equations that appear that also use pipes and
	 * some have 2 pipes, so we need more sophisticated match than just counting
	 * pipes
	 * 
	 * @param line
	 * @return
	 */
	private static boolean lineIsAbbreviation(String line) {
		// return line.split("\\|").length == 3;

		Pattern p = Pattern.compile("^\\s\\s.*?\\|.*?\\|(.*?)$");
		Matcher m = p.matcher(line);
		if (m.find()) {
			String number = m.group(1);

			try {
				Float.parseFloat(number);
				return true;
			} catch (IllegalArgumentException e) {
				// if we can't parse this, then it's not a number, and this is not an
				// abbreviation line
				return false;
			}
		}
		return false;

	}

//	public static PCollectionTuple process(
//			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToSentenceBionlp,
//			SubProcessConfiguration subprocessConfig, String abbreviationsBinaryName,
//			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {
//
//		return statusEntityToSentenceBionlp.apply("Identify concept annotations in sentences", ParDo.of(
//				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(
//							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
//							MultiOutputReceiver out) {
//						ProcessingStatus processingStatus = statusEntityToText.getKey();
//						String docId = processingStatus.getDocumentId();
//
//						Entry<DocumentCriteria, String> entry = statusEntityToText.getValue().entrySet().iterator()
//								.next();
//
//						try {
//							// single entry should be sentences in bionlp format
//							if (entry.getKey().getDocumentType() == DocumentType.SENTENCE) {
//
//								String sentenceAnnotsInBioNLP = entry.getValue();
//
//								// format returned is annotations in bionlp with an extra column 0 that contains
//								// the document id. This is for future use in possibly batching RPCs.
//								String crfOutputInBionlpPlusDocId = annotate(sentenceAnnotsInBioNLP, docId,
//										crfServiceUri);
//
//								String crfOutputInBionlp = extractBionlp(crfOutputInBionlpPlusDocId);
//
//								List<String> chunkedCrfOutput = PipelineMain.chunkContent(crfOutputInBionlp);
//								out.get(ABBREVIATIONS_TAG).output(KV.of(processingStatus, chunkedCrfOutput));
//
//							} else {
//								throw new IllegalArgumentException(
//										"Unable to process CRF NER as sentences are missing.");
//							}
//						} catch (Throwable t) {
//							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
//									"Failure during OGER annotation.", docId, t, timestamp);
//							out.get(ETL_FAILURE_TAG).output(failure);
//						}
//
//					}
//
//				}).withOutputTags(ABBREVIATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
//	}
//
//	@VisibleForTesting
//	protected static String extractBionlp(String crfOutputJson) {
//		Gson gson = new Gson();
//		Type type = new TypeToken<Map<String, Map<String, String>>>() {
//		}.getType();
//		Map<String, Map<String, String>> outerMap = gson.fromJson(crfOutputJson, type);
//
//		// there should only be one entry in the outer map and one entry in the inner
//		// map
//		Map<String, String> innerMap = outerMap.entrySet().iterator().next().getValue();
//		if (innerMap.size() == 0) {
//			return "";
//		}
//		String crfOutputInBionlp = innerMap.entrySet().iterator().next().getValue();
//		return crfOutputInBionlp;
//	}
//
//	@VisibleForTesting
//	protected static String removeFirstColumn(String crfOutputInBionlpPlusDocId) {
//		StringBuilder sb = new StringBuilder();
//		for (String line : crfOutputInBionlpPlusDocId.split("\\n")) {
//			sb.append(line.substring(line.indexOf("\t") + 1) + "\n");
//		}
//		return sb.toString();
//	}
//
//	public static String annotate(String sentenceAnnotsInBioNLP, String docId, String crfServiceUri)
//			throws IOException {
//
//		// add doc id
//		String withDocId = addLeadingColumn(sentenceAnnotsInBioNLP, docId);
//		String targetUri = String.format("%s/crf", crfServiceUri);
//
//		return new HttpPostUtil(targetUri).submit(withDocId);
//	}
//
//	@VisibleForTesting
//	protected static String addLeadingColumn(String sentenceAnnotsInBioNLP, String docId) {
//		StringBuilder sb = new StringBuilder();
//		for (String line : sentenceAnnotsInBioNLP.split("\\n")) {
//			if (!line.trim().isEmpty()) {
//				sb.append(docId + "\t" + line + "\n");
//			}
//		}
//		return sb.toString();
//	}

}
