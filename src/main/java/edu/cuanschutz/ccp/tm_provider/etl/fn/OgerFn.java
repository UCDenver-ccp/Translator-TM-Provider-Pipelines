package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
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

	public enum OgerOutputType {
		/**
		 * OGER used by itself (without BioBERT) can return
		 */
		TSV, PUBANNOTATION
	}

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText, String ogerServiceUri,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp, OgerOutputType ogerOutputType) {

		return statusEntityToText.apply("Identify concept annotations", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();
						// there is only one entry in the input map and it is the plain text of the
						// input document
						String plainText = statusEntityToText.getValue().entrySet().iterator().next().getValue();

						try {
							String ogerOutput = annotate(plainText, ogerServiceUri, ogerOutputType);

							if (outputDocCriteria.getDocumentFormat() == DocumentFormat.BIONLP) {
								ogerOutput = convertToBioNLP(ogerOutput, docId, plainText, ogerOutputType);
							}

							List<String> chunkedOgerOutput = PipelineMain.chunkContent(ogerOutput);
							out.get(ANNOTATIONS_TAG).output(KV.of(statusEntity, chunkedOgerOutput));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during OGER annotation.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}

				}).withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
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
			if (!SpanValidator.validate(ta.getSpans(), ta.getCoveredText(), docText)) {
				throw new IllegalStateException(String.format(
						"OGER span mismatch detected. doc_id: %s span: %s expected_text: %s observed_text: %s", docId,
						ta.getSpans().toString(), ta.getCoveredText(),
						SpanUtils.getCoveredText(ta.getSpans(), docText)));
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
