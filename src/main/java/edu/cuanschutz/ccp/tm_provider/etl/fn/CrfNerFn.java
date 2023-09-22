package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

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
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.MultithreadedServiceCalls;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.HttpPostUtil;
import lombok.Getter;

public class CrfNerFn extends DoFn<KV<String, String>, KV<String, String>> {

	private final static Logger LOGGER = Logger.getLogger(CrfNerFn.class.getName());

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> CRAFT_NER_ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> NLMDISEASE_NER_ANNOTATIONS_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToSentenceBionlp,
			String craftCrfServiceUri, String nlmDiseaseCrfServiceUri, DocumentCriteria outputDocCriteria,
			com.google.cloud.Timestamp timestamp, MultithreadedServiceCalls multithreadedServiceCalls) {

		return statusEntityToSentenceBionlp.apply("Identify concept annotations in sentences", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus processingStatus = statusEntityToText.getKey();
						String docId = processingStatus.getDocumentId();

						try {
							String augmentedSentenceBionlp = PipelineMain
									.getAugmentedSentenceBionlp(statusEntityToText.getValue(), docId);

							// there are cases where the document text is empty
							if (!augmentedSentenceBionlp.trim().isEmpty()) {

								// possible to parallelize the service calls -- see below -- the only drawback
								// is that when there's an exception the cause seems to get eaten and it's
								// difficult to debug. If you want to debug - run with the
								// multithreadedServiceCalls flag set to false.
								if (multithreadedServiceCalls == MultithreadedServiceCalls.ENABLED) {
									ServiceCaller craftServiceCaller = new ServiceCaller(craftCrfServiceUri, docId,
											augmentedSentenceBionlp);
									ServiceCaller nlmDiseaseServiceCaller = new ServiceCaller(nlmDiseaseCrfServiceUri,
											docId, augmentedSentenceBionlp);

									// execute crf service calls in parallel and wait for them to finish
									ExecutorService executor = Executors.newFixedThreadPool(5);
									List<Future<?>> futures = Arrays.asList(executor.submit(craftServiceCaller),
											executor.submit(nlmDiseaseServiceCaller));
									for (Future<?> f : futures) {
										f.get();
									}

									List<String> craftChunkedCrfOutput = craftServiceCaller.getChunkedCrfOutput();
									if (craftChunkedCrfOutput != null && !craftChunkedCrfOutput.isEmpty()) {
										out.get(CRAFT_NER_ANNOTATIONS_TAG)
												.output(KV.of(processingStatus, craftChunkedCrfOutput));
									}

									List<String> nlmDiseaseChunkedCrfOutput = nlmDiseaseServiceCaller
											.getChunkedCrfOutput();
									if (nlmDiseaseChunkedCrfOutput != null && !nlmDiseaseChunkedCrfOutput.isEmpty()) {
										out.get(NLMDISEASE_NER_ANNOTATIONS_TAG)
												.output(KV.of(processingStatus, nlmDiseaseChunkedCrfOutput));
									}
								} else {
									callCrfService(CRAFT_NER_ANNOTATIONS_TAG, craftCrfServiceUri, out, processingStatus,
											docId, augmentedSentenceBionlp);
									callCrfService(NLMDISEASE_NER_ANNOTATIONS_TAG, nlmDiseaseCrfServiceUri, out,
											processingStatus, docId, augmentedSentenceBionlp);
								}
							} else {
								LOGGER.warning(String.format(
										"Skipping CRF processing of document %s b/c there is no document text.",
										docId));
							}

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during CRF annotation.", docId, t, timestamp);
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
					private void callCrfService(TupleTag<KV<ProcessingStatus, List<String>>> tag, String crfServiceUri,
							MultiOutputReceiver out, ProcessingStatus processingStatus, String docId,
							String augmentedSentenceBionlp) throws IOException, UnsupportedEncodingException {
						String s = augmentedSentenceBionlp.replaceAll("\\n", "").trim();
						if (!s.isEmpty()) {
							String crfOutputInBionlpPlusDocId = annotate(augmentedSentenceBionlp, docId, crfServiceUri);
							String crfOutputInBionlp = extractBionlp(crfOutputInBionlpPlusDocId);
							List<String> chunkedCrfOutput = PipelineMain.chunkContent(crfOutputInBionlp);
							out.get(tag).output(KV.of(processingStatus, chunkedCrfOutput));
						}
					}

				}).withOutputTags(CRAFT_NER_ANNOTATIONS_TAG,
						TupleTagList.of(ETL_FAILURE_TAG).and(NLMDISEASE_NER_ANNOTATIONS_TAG)));
	}

	private static class ServiceCaller implements Runnable {

		private final String crfServiceUri;
		private final String docId;
		private final String augmentedSentenceBionlp;
		@Getter
		private List<String> chunkedCrfOutput;

		public ServiceCaller(String crfServiceUri, String docId, String augmentedSentenceBionlp) {
			this.crfServiceUri = crfServiceUri;
			this.docId = docId;
			this.augmentedSentenceBionlp = augmentedSentenceBionlp;
		}

		@Override
		public void run() {

			try {
				String s = augmentedSentenceBionlp.replaceAll("\\n", "").trim();
				if (!s.isEmpty()) {
					String crfOutputInBionlpPlusDocId = annotate(augmentedSentenceBionlp, docId, crfServiceUri);
					String crfOutputInBionlp = extractBionlp(crfOutputInBionlpPlusDocId);
					chunkedCrfOutput = PipelineMain.chunkContent(crfOutputInBionlp);
				}
			} catch (IOException e) {
				throw new IllegalStateException(
						String.format("Error during CRF service call for document ID: %s", docId), e);
			}
		}

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

		// there are cases where there are blank lines in the text (usually near/in a
		// table in the text). These cause IndexOutOfBoundsExceptions because there is
		// no sentence text. In these cases we will add a tab so that an empty
		// placeholder for the sentence text will exist.
//		StringBuilder sb = new StringBuilder();
//		for (String line : withDocId.split("\\n")) {
//			String[] cols = line.split("\\t");
//			// a properly formatted line will have 4 tab-separated columns
//			if (cols.length == 3) {
//				line = line + "\t";
//			}
//			sb.append(line + "\n");
//		}
//		
//		withDocId = sb.toString();

//		// debugging index OOB exception
//		for (String line : withDocId.split("\\n")) {
//			try {
//				String[] cols = line.split("\\t", -1);
//				String documentId = cols[0];
//				String annotId = cols[1];
//				String coveredText = cols[3];
//
//				String[] typeSpan = cols[2].split(" ");
//				String type = typeSpan[0];
//				int spanStart = Integer.parseInt(typeSpan[1]);
//				int spanEnd = Integer.parseInt(typeSpan[2]);
//			} catch (IndexOutOfBoundsException e) {
//				throw new IllegalStateException(
//						"IOB Exception detected on sentence line: " + line.replaceAll("\\t", " [TAB] "), e);
//			}
//		}
//		// end debugging

		String targetUri = String.format("%s/crf", crfServiceUri);

		return new HttpPostUtil(targetUri).submit(withDocId);
	}

	@VisibleForTesting
	protected static String addLeadingColumn(String sentenceAnnotsInBioNLP, String docId) {
		StringBuilder sb = new StringBuilder();
		for (String line : sentenceAnnotsInBioNLP.split("\\n")) {
			if (!line.trim().isEmpty()) {
				sb.append(docId + "\t" + line + "\n");
			}
		}
		return sb.toString();
	}

}
