package edu.cuanschutz.ccp.tm_provider.etl.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;

public class SentenceTsvBuilderFn extends DoFn<KV<ProcessingStatus, ExtractedSentence>, KV<ProcessingStatus, String>> {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, String>> OUTPUT_TSV_TAG = new TupleTag<KV<ProcessingStatus, String>>() {
	};

	// TODO
//	add new output tag - format will be Turku comment line with sentence ID followed by sentence on the next line
//	
//	also add BERT output format?????
//			

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<KV<ProcessingStatus, ExtractedSentence>> extractedSentences,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return extractedSentences.apply("extracted sent->tsv",
				ParDo.of(new DoFn<KV<ProcessingStatus, ExtractedSentence>, KV<ProcessingStatus, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c, MultiOutputReceiver out) {
						KV<ProcessingStatus, ExtractedSentence> element = c.element();
						try {
							String tsv = element.getValue().toTsv();
							if (tsv != null) {
								out.get(OUTPUT_TSV_TAG).output(KV.of(element.getKey(), tsv));
							}
						} catch (Throwable t) {
							String docId = element.getValue().getDocumentId();

							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during sentence extraction.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}

				}).withOutputTags(OUTPUT_TSV_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

}
