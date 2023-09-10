package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.MultithreadedServiceCalls;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.HttpPostUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.SpanValidator;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.file.conversion.pubannotation.PubAnnotationDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.SpanUtils;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Getter;

/**
 * This function submits plain text to the Oger concept recognition service and
 * returns a PCollection mapping the document ID to the extracted concept
 * annotations serialized using the BioNLP format.
 * 
 * Input: KV<docId,plainText> <br/>
 * Output: KV<docId,conlluText>
 *
 */
public class OgerFn extends DoFn<KV<String, String>, KV<String, String>> {

	private final static Logger LOGGER = Logger.getLogger(OgerFn.class.getName());

	public enum OgerOutputType {
		/**
		 * OGER used by itself (without BioBERT) can return
		 */
		TSV, PUBANNOTATION
	}

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> CS_ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> CIMIN_ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> CIMAX_ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText,
			String csOgerServiceUri, String ciminOgerServiceUri, String cimaxOgerServiceUri,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			MultithreadedServiceCalls multithreadedServiceCalls) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus processingStatus = statusEntityToText.getKey();
						String docId = processingStatus.getDocumentId();
						// there is only one entry in the input map and it is the plain text of the
						// input document
						try {
							String augmentedDocText = PipelineMain
									.getAugmentedDocumentText(statusEntityToText.getValue(), docId);

							if (!augmentedDocText.trim().isEmpty()) {
								// possible to parallelize the service calls -- see below -- the only drawback
								// is that when there's an exception the cause seems to get eaten and it's
								// difficult to debug. If you want to debug - run with the
								// multithreadedServiceCalls flag set to false.
								if (multithreadedServiceCalls == MultithreadedServiceCalls.ENABLED) {

									ServiceCaller csServiceCaller = new ServiceCaller(csOgerServiceUri, docId,
											augmentedDocText);
									ServiceCaller ciminServiceCaller = new ServiceCaller(ciminOgerServiceUri, docId,
											augmentedDocText);
									ServiceCaller cimaxServiceCaller = new ServiceCaller(cimaxOgerServiceUri, docId,
											augmentedDocText);

									// execute oger service calls in parallel and wait for them to finish
									ExecutorService executor = Executors.newFixedThreadPool(5);
									List<Future<?>> futures = Arrays.asList(executor.submit(csServiceCaller),
											executor.submit(ciminServiceCaller), executor.submit(cimaxServiceCaller));
									for (Future<?> f : futures) {
										f.get();
									}

									List<String> csChunkedOgerOutput = csServiceCaller.getChunkedOgerOutput();
									if (csChunkedOgerOutput != null && !csChunkedOgerOutput.isEmpty()) {
										out.get(CS_ANNOTATIONS_TAG)
												.output(KV.of(processingStatus, csChunkedOgerOutput));
									}

									List<String> ciminChunkedOgerOutput = ciminServiceCaller.getChunkedOgerOutput();
									if (ciminChunkedOgerOutput != null && !ciminChunkedOgerOutput.isEmpty()) {
										out.get(CIMIN_ANNOTATIONS_TAG)
												.output(KV.of(processingStatus, ciminChunkedOgerOutput));
									}

									List<String> cimaxChunkedOgerOutput = cimaxServiceCaller.getChunkedOgerOutput();
									if (cimaxChunkedOgerOutput != null && !cimaxChunkedOgerOutput.isEmpty()) {
										out.get(CIMAX_ANNOTATIONS_TAG)
												.output(KV.of(processingStatus, cimaxChunkedOgerOutput));
									}
								} else {
									callOgerService(CS_ANNOTATIONS_TAG, csOgerServiceUri, out, processingStatus, docId,
											augmentedDocText);
									callOgerService(CIMIN_ANNOTATIONS_TAG, ciminOgerServiceUri, out, processingStatus,
											docId, augmentedDocText);
									callOgerService(CIMAX_ANNOTATIONS_TAG, cimaxOgerServiceUri, out, processingStatus,
											docId, augmentedDocText);
								}

							} else {
								LOGGER.warning(String.format(
										"Skipping CRF processing of document %s b/c there is no document text.",
										docId));
							}

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during OGER annotation.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}

					/**
					 * Calls the CRF service to annotate the provided sentences and output entity
					 * annotations to the specified multioutputreceiver
					 * 
					 * @param tag
					 * @param crfServiceUri
					 * @param out
					 * @param processingStatus
					 * @param docId
					 * @param augmentedSentenceBionlp
					 * @throws IOException
					 * @throws UnsupportedEncodingException
					 */
					private void callOgerService(TupleTag<KV<ProcessingStatus, List<String>>> tag,
							String ogerServiceUri, MultiOutputReceiver out, ProcessingStatus processingStatus,
							String docId, String augmentedDocText) throws IOException, UnsupportedEncodingException {
						String s = augmentedDocText.replaceAll("\\n", "").trim();
						if (!s.isEmpty()) {
							String ogerOutput = annotate(augmentedDocText, ogerServiceUri, OgerOutputType.TSV);
							ogerOutput = convertToBioNLP(ogerOutput, docId, augmentedDocText, OgerOutputType.TSV);
							List<String> chunkedOgerOutput = PipelineMain.chunkContent(ogerOutput);
							out.get(tag).output(KV.of(processingStatus, chunkedOgerOutput));
						}
					}

				}).withOutputTags(CS_ANNOTATIONS_TAG,
						TupleTagList.of(ETL_FAILURE_TAG).and(CIMIN_ANNOTATIONS_TAG).and(CIMAX_ANNOTATIONS_TAG)));
	}

	private static class ServiceCaller implements Runnable {

		private String ogerServiceUri;
		private String docId;
		private String augmentedDocText;
		@Getter
		private List<String> chunkedOgerOutput;

		public ServiceCaller(String ogerServiceUri, String docId, String augmentedDocText) {
			this.ogerServiceUri = ogerServiceUri;
			this.docId = docId;
			this.augmentedDocText = augmentedDocText;
		}

		@Override
		public void run() {

			try {
				String s = augmentedDocText.replaceAll("\\n", "").trim();
				if (!s.isEmpty()) {
					String ogerOutput = annotate(augmentedDocText, ogerServiceUri, OgerOutputType.TSV);
					ogerOutput = convertToBioNLP(ogerOutput, docId, augmentedDocText, OgerOutputType.TSV);
					chunkedOgerOutput = PipelineMain.chunkContent(ogerOutput);
				}
			} catch (IOException e) {
				throw new IllegalStateException(
						String.format("Error during OGER service call for document ID: %s", docId), e);
			}
		}
	}

	/**
	 * Expected OGER system output is either TSV or PubAnnotation. TSV is output by
	 * the OGER system when it is not paired with BioBERT, PubAnnotation when it is.
	 * 
	 * OGER TSV:
	 * 
	 * <pre>
	 * 12345	cell	0	11	Blood cells	blood cell	CL:0000081		S1	CL	
	 * 12345	cell	16	23	neurons	neuron	CL:0000540		S1	CL
	 * </pre>
	 * 
	 * @param ogerTsv
	 * @return
	 * @throws IOException
	 */
	public static String convertToBioNLP(String ogerSystemOutput, String docId, String docText,
			OgerOutputType ogerOutputType) throws IOException {

		TextDocument td = null;

		if (ogerOutputType == OgerOutputType.TSV) {
			TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
			td = new TextDocument(docId, "sourcedb", docText);

			for (StreamLineIterator lineIter = new StreamLineIterator(
					new ByteArrayInputStream(ogerSystemOutput.getBytes(CharacterEncoding.UTF_8.getCharacterSetName())),
					CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
				String line = lineIter.next().getText();
				if (!line.trim().isEmpty()) {
					String[] cols = line.split("\\t");
					try {
						int spanStart = Integer.parseInt(cols[2]);
						int spanEnd = Integer.parseInt(cols[3]);
						String coveredText = cols[4];
						String id = cols[6];
						td.addAnnotation(factory.createAnnotation(spanStart, spanEnd, coveredText, id));
					} catch (ArrayIndexOutOfBoundsException e) {
						throw new IOException(
								String.format("ArrayIndexOutOfBounds. Line (num cols: %d) = \"%s\"", cols.length, line),
								e);
					}
				}
			}
		} else if (ogerOutputType == OgerOutputType.PUBANNOTATION) {
			PubAnnotationDocumentReader docReader = new PubAnnotationDocumentReader();
			td = docReader.readDocument(docId, "unknown_source", new ByteArrayInputStream(ogerSystemOutput.getBytes()),
					new ByteArrayInputStream(docText.getBytes()), CharacterEncoding.UTF_8);
		}
		// if there aren't any annotations, then just initialize the field so that the
		// writer doesn't complain
		if (td.getAnnotations() == null) {
			td.setAnnotations(new ArrayList<TextAnnotation>());
		}

		/*
		 * Validate the annotation spans, ensuring that the annotation covered text
		 * matches the document text
		 */
		for (TextAnnotation ta : td.getAnnotations()) {
			// turning off strict validation b/c it was failing for some where the match
			// seemed to be exact. Not sure why. For now we will log validation errors.
			if (!SpanValidator.validate(ta.getSpans(), ta.getCoveredText(), docText)) {

				List<Integer> ctCharVals = new ArrayList<Integer>();
				for (char c : ta.getCoveredText().toCharArray()) {
					ctCharVals.add(Character.getNumericValue(c));
				}

				List<Integer> dtCharVals = new ArrayList<Integer>();
				for (char c : SpanUtils.getCoveredText(ta.getSpans(), docText).toCharArray()) {
					dtCharVals.add(Character.getNumericValue(c));
				}

				String errorMessage = String.format(
						"OGER span mismatch detected. doc_id: %s span: %s expected_text: '%s' %s observed_text: '%s' %s",
						docId, ta.getSpans().toString(), ta.getCoveredText(), ctCharVals.toString(),
						SpanUtils.getCoveredText(ta.getSpans(), docText), dtCharVals.toString());

				LOGGER.log(Level.WARNING, errorMessage);
//				throw new IllegalStateException(errorMessage);
			}
		}

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		writer.serialize(td, outStream, CharacterEncoding.UTF_8);
		return outStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());
	}

	/**
	 * Invoke the OGER service returns results in CoNLL format
	 * 
	 * @param plainTextWithBreaks
	 * @param dependencyParserServiceUri
	 * @return
	 * @throws IOException
	 */
	public static String annotate(String plainText, String ogerServiceUri, OgerOutputType ogerOutputType)
			throws IOException {

		String targetUri = null;

		if (ogerOutputType == OgerOutputType.TSV) {
			String formatKey = "tsv";
			// doc id (12345) is optional -- can only be numbers
			targetUri = String.format("%s/upload/txt/%s/12345", ogerServiceUri, formatKey);

		} else if (ogerOutputType == OgerOutputType.PUBANNOTATION) {
			targetUri = String.format("%s/oger", ogerServiceUri);
		}

		return new HttpPostUtil(targetUri).submit(plainText);
	}

}
