package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.HttpPostUtil;

public class CrfNerFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToSentenceBionlp,
			String crfServiceUri, DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return statusEntityToSentenceBionlp.apply("Identify concept annotations in sentences", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus processingStatus = statusEntityToText.getKey();
						String docId = processingStatus.getDocumentId();

						Entry<DocumentCriteria, String> entry = statusEntityToText.getValue().entrySet().iterator()
								.next();

						try {
							// single entry should be sentences in bionlp format
							if (entry.getKey().getDocumentType() == DocumentType.SENTENCE) {

								String sentenceAnnotsInBioNLP = entry.getValue();

								// format returned is annotations in bionlp with an extra column 0 that contains
								// the document id. This is for future use in possibly batching RPCs.
								String crfOutputInBionlpPlusDocId = annotate(sentenceAnnotsInBioNLP, docId,
										crfServiceUri);

								String crfOutputInBionlp = extractBionlp(crfOutputInBionlpPlusDocId);

								List<String> chunkedCrfOutput = PipelineMain.chunkContent(crfOutputInBionlp);
								out.get(ANNOTATIONS_TAG).output(KV.of(processingStatus, chunkedCrfOutput));

							} else {
								throw new IllegalArgumentException(
										"Unable to process CRF NER as sentences are missing.");
							}
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during OGER annotation.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}

					}

				}).withOutputTags(ANNOTATIONS_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	@VisibleForTesting
	protected static String extractBionlp(String crfOutputJson) {
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Map<String, String>>>() {
		}.getType();
		Map<String, Map<String, String>> outerMap = gson.fromJson(crfOutputJson, type);

		// there should only be one entry in the outer map and one entry in the inner
		// map
		Map<String, String> innerMap = outerMap.entrySet().iterator().next().getValue();
		if (innerMap.size() == 0) {
			return "";
		}
		String crfOutputInBionlp = innerMap.entrySet().iterator().next().getValue();
		return crfOutputInBionlp;
	}

	@VisibleForTesting
	protected static String removeFirstColumn(String crfOutputInBionlpPlusDocId) {
		StringBuilder sb = new StringBuilder();
		for (String line : crfOutputInBionlpPlusDocId.split("\\n")) {
			sb.append(line.substring(line.indexOf("\t") + 1) + "\n");
		}
		return sb.toString();
	}

	public static String annotate(String sentenceAnnotsInBioNLP, String docId, String crfServiceUri)
			throws IOException {

		// add doc id
		String withDocId = addLeadingColumn(sentenceAnnotsInBioNLP, docId);
		String targetUri = String.format("%s/crf", crfServiceUri);

		return new HttpPostUtil(targetUri).submit(withDocId);
	}

	@VisibleForTesting
	protected static String addLeadingColumn(String sentenceAnnotsInBioNLP, String docId) {
		StringBuilder sb = new StringBuilder();
		for (String line : sentenceAnnotsInBioNLP.split("\\n")) {
			sb.append(docId + "\t" + line + "\n");
		}
		return sb.toString();
	}

}
