package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.TextExtractionPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.ucdenver.ccp.nlp.core.annotation.Span;

/**
 * Used to output extracted sentences (which are sentences with specific concept
 * pairings in them) to a format that will be processed using a dependency
 * parser. This format allows comments that are passed along to the output that
 * will contain the document identifier and some sentence metadata, e.g.,
 * sentence span. In order to aid the dependency parser, multi-token concepts
 * will be connected with underscores, e.g., red blood cells = red_blood_cells
 */
public class SentenceDpInputBuilderFn
		extends DoFn<KV<ProcessingStatus, ExtractedSentence>, KV<ProcessingStatus, String>> {

	private static final long serialVersionUID = 1L;
	public static final String SENTENCE_COMMENT_INDICATOR = "SENTENCE";
	public static final String ENTITY_COMMENT_INDICATOR = "ENTITY";
	public static final String OTHER_ENTITY_COMMENT_INDICATOR = "OTHER_ENTITY";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, String>> OUTPUT_SENTENCE_FOR_DP_TAG = new TupleTag<KV<ProcessingStatus, String>>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<KV<ProcessingStatus, ExtractedSentence>> extractedSentences,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp) {

		return extractedSentences.apply("extracted sent->dp input",
				ParDo.of(new DoFn<KV<ProcessingStatus, ExtractedSentence>, KV<ProcessingStatus, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c, MultiOutputReceiver out) {
						KV<ProcessingStatus, ExtractedSentence> element = c.element();
						try {
							ExtractedSentence es = element.getValue();
							if (es != null) {
								String sentenceWithComments = getSentenceWithComments(es);
								out.get(OUTPUT_SENTENCE_FOR_DP_TAG)
										.output(KV.of(element.getKey(), sentenceWithComments));
							}
						} catch (Throwable t) {
							String docId = element.getValue().getDocumentId();

							EtlFailureData failure = new EtlFailureData(outputDocCriteria,
									"Failure during sentence extraction.", docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}

				}).withOutputTags(OUTPUT_SENTENCE_FOR_DP_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * Creates a multi-line string. This first line is a comment (using the format
	 * recognized by the Turku Dependency Parser). The comment will include metadata
	 * about the sentence: Document_id [tab] sentence_identifier [tab]
	 * sentence_span_start
	 * 
	 * The second line contains the sentence with any multi-token concepts connected
	 * via underscores, e.g. red blood cells = red_blood_cells
	 * 
	 * @param es
	 * @return
	 */
	protected static String getSentenceWithComments(ExtractedSentence es) {
		StringBuilder sb = new StringBuilder();

		String commentLine = String.format("%s%s\t%s\t%s\t%d\n", TextExtractionPipeline.COMMENT_INDICATOR,
				SENTENCE_COMMENT_INDICATOR, es.getDocumentId(), es.getSentenceIdentifier(), es.getSentenceSpanStart());
		String entity1SpanStr = getSpanString(es.getEntitySpan1());
		String entity1Line = String.format("%s%s\t%s\t%s\t%s\n", TextExtractionPipeline.COMMENT_INDICATOR,
				ENTITY_COMMENT_INDICATOR, es.getEntityId1(), entity1SpanStr, es.getEntityCoveredText1());
		String entity2SpanStr = getSpanString(es.getEntitySpan2());
		String entity2Line = String.format("%s%s\t%s\t%s\t%s\n", TextExtractionPipeline.COMMENT_INDICATOR,
				ENTITY_COMMENT_INDICATOR, es.getEntityId2(), entity2SpanStr, es.getEntityCoveredText2());

		sb.append(commentLine);
		sb.append(entity1Line);
		sb.append(entity2Line);

		/*
		 * create a comment line for all concept entities (right now this duplicates the
		 * two entities already listed)
		 */
		for (int i = 0; i < es.getOtherEntityIds().size(); i++) {
			String conceptId = es.getOtherEntityIds().get(i);
			if (!conceptId.trim().isEmpty()) {
				String coveredText = es.getOtherEntityCoveredText().get(i);
				List<Span> spans = es.getOtherEntitySpans().get(i);

				String spanStr = getSpanString(spans);
				String entityLine = String.format("%s%s\t%s\t%s\t%s\n", TextExtractionPipeline.COMMENT_INDICATOR,
						OTHER_ENTITY_COMMENT_INDICATOR, conceptId, spanStr, coveredText);

				sb.append(entityLine);
			}
		}

		sb.append(es.getSentenceTextWithUnderscoredEntities() + "\n\n");

		return sb.toString();
	}

	/**
	 * @param spans
	 * @return a string representation of the span list whereby the start and end
	 *         are separated by a pipe and distinct spans (if there are more than
	 *         one) are separated by a semi-colon
	 */
	private static String getSpanString(List<Span> spans) {
		Collections.sort(spans, Span.ASCENDING());
		StringBuilder sb = new StringBuilder();
		for (Span span : spans) {
			if (sb.length() > 0) {
				sb.append(";");
			}
			sb.append(span.getSpanStart() + "|" + span.getSpanEnd());
		}
		return sb.toString();
	}

}
