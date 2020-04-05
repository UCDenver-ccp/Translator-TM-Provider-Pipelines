package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.HttpPostUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
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

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> ANNOTATIONS_TAG = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToText, String ogerServiceUri,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return docIdToText.apply("Identify concept annotations",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToTextKV, MultiOutputReceiver out) {
						String docId = docIdToTextKV.getKey();
						String plainText = docIdToTextKV.getValue();

						try {
							String ogerOutput = annotate(plainText, ogerServiceUri,
									outputDocCriteria.getDocumentFormat());

							if (outputDocCriteria.getDocumentFormat() == DocumentFormat.BIONLP) {
								ogerOutput = convertToBioNLP(ogerOutput, docId, plainText);
							}

							List<String> chunkedOgerOutput = PipelineMain.chunkContent(ogerOutput);
							out.get(ANNOTATIONS_TAG).output(KV.of(docId, chunkedOgerOutput));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during OGER annotation.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}

				}).withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * Converts from the OGER TSV annotation format to the BioNLP annotation format
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
	private static String convertToBioNLP(String ogerTsv, String docId, String docText) throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(docId);
		TextDocument td = new TextDocument(docId, "sourcedb", docText);

		for (StreamLineIterator lineIter = new StreamLineIterator(new ByteArrayInputStream(ogerTsv.getBytes()),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
			String line = lineIter.next().getText();
			String[] cols = line.split("\\t");
			int spanStart = Integer.parseInt(cols[2]);
			int spanEnd = Integer.parseInt(cols[3]);
			String coveredText = cols[4];
			String id = cols[6];
			td.addAnnotation(factory.createAnnotation(spanStart, spanEnd, coveredText, id));
		}

		// if there aren't any annotations, then just initialize the field so that the
		// writer doesn't complain
		if (td.getAnnotations() == null) {
			td.setAnnotations(new ArrayList<TextAnnotation>());
		}

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		writer.serialize(td, outStream, CharacterEncoding.UTF_8);
		return outStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());
	}

	/**
	 * Invoke the OGER service returns results in a tab-delimited format.
	 * 
	 * @param plainTextWithBreaks
	 * @param dependencyParserServiceUri
	 * @return
	 * @throws IOException
	 */
	private static String annotate(String plainText, String ogerServiceUri, DocumentFormat outputFormat)
			throws IOException {
		String formatKey = "tsv";
		if (outputFormat == DocumentFormat.BIONLP) {
			/* the tsv output will be converted to BioNLP format */
			formatKey = "tsv";
		} else if (outputFormat == DocumentFormat.OGER_CONLL) {
			formatKey = "conll";
		}
		// doc id (12345) is optional -- can only be numbers
		String targetUri = String.format("%s/upload/txt/%s/12345", ogerServiceUri, formatKey);
		return new HttpPostUtil(targetUri).submit(plainText);
	}

}
