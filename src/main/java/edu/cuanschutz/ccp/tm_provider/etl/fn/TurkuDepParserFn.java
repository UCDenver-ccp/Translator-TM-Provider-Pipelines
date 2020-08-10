package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
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
import edu.cuanschutz.ccp.tm_provider.etl.util.HttpPostUtil;

/**
 * This function submits plain text to the Turku neural dependency parser
 * service and returns a PCollection mapping the document ID to CoNLL-U output
 * from the Turku neural dependency parser.
 * 
 * Input: KV<docId,plainText> <br/>
 * Output: KV<docId,conlluText>
 *
 */
public class TurkuDepParserFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole CoNLLU string is too large to store in Datastore. The list allows it to
	 * be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> CONLLU_TAG = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToBiocXml,
			String dependencyParserServiceUri, DocumentCriteria dc, com.google.cloud.Timestamp timestamp) {

		return docIdToBiocXml.apply("Compute dependency parse",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToText, MultiOutputReceiver out) {
						String docId = docIdToText.getKey();
						String plainText = docIdToText.getValue();

						/*
						 * the turku parser treats blank lines as section separators. Single line breaks
						 * are not treated as separators. The input text has one paragraph per line, so
						 * we will add extra line breaks to the text here before submitting it to the
						 * Turku parser service.
						 */

						String plainTextWithBreaks = plainText.replaceAll("\\n", "\n\n");

						try {
							String conllu = parseText(plainTextWithBreaks, dependencyParserServiceUri);

							/*
							 * divide the document content into chunks if necessary so that each chunk is
							 * under the DataStore byte length threshold
							 */
							List<String> chunkedConllu = PipelineMain.chunkContent(conllu);
							out.get(CONLLU_TAG).output(KV.of(docId, chunkedConllu));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(dc, "Failure during dependency parsing.", docId,
									t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}
				}).withOutputTags(CONLLU_TAG, TupleTagList.of(ETL_FAILURE_TAG)));// .and(processingStatusTag)));
	}

	/**
	 * Invoke the Turku neural dependency parser service and return the resulting
	 * parse in CoNLL-U format.
	 * 
	 * @param plainTextWithBreaks
	 * @param dependencyParserServiceUri
	 * @return
	 * @throws IOException
	 */
	private static String parseText(String plainTextWithBreaks, String dependencyParserServiceUri) throws IOException {
		return new HttpPostUtil(dependencyParserServiceUri).submit(plainTextWithBreaks);
	}

}
