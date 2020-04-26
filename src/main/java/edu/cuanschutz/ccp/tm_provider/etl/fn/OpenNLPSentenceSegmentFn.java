package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.impl.DefaultTextAnnotation;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.Span;

/**
 * This function takes plain text as input and uses the OpenNLP sentence
 * segmenter to return a PCollection mapping the document ID to sentences using
 * BioNLP format.
 * 
 * Input: KV<docId,plainText> <br/>
 * Output: KV<docId,sentenceBioNLP>
 *
 */
public class OpenNLPSentenceSegmentFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole sentence annotation document is too large to store in Datastore. The
	 * list allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> SENTENCE_ANNOT_TAG = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToInputText, DocumentCriteria dc,
			com.google.cloud.Timestamp timestamp) {

		return docIdToInputText.apply("Segment sentences",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToText, MultiOutputReceiver out) {
						String docId = docIdToText.getKey();
						String plainText = docIdToText.getValue();

						try {
							String bionlp = segmentSentences(plainText);

							/*
							 * divide the document content into chunks if necessary so that each chunk is
							 * under the DataStore byte length threshold
							 */
							List<String> chunkedConllu = PipelineMain.chunkContent(bionlp);
							out.get(SENTENCE_ANNOT_TAG).output(KV.of(docId, chunkedConllu));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(dc, "Failure during sentence segmentation.",
									docId, t, timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}
				}).withOutputTags(SENTENCE_ANNOT_TAG, TupleTagList.of(ETL_FAILURE_TAG)));// .and(processingStatusTag)));
	}

	/**
	 * Use the OpenNLP sentence segmenter to segment sentences. Return sentence
	 * annotations in the BioNLP format.
	 * 
	 * @param plainTextWithBreaks
	 * @param dependencyParserServiceUri
	 * @return
	 * @throws IOException
	 */
	private static String segmentSentences(String plainText) throws IOException {

		InputStream modelStream = ClassPathUtil.getResourceStreamFromClasspath(OpenNLPSentenceSegmentFn.class,
				"/de/tudarmstadt/ukp/dkpro/core/opennlp/lib/sentence-en-maxent.bin");
			SentenceModel model = new SentenceModel(modelStream);
			SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
		
		List<TextAnnotation> annots = new ArrayList<TextAnnotation>();
		Span[] spans = sentenceDetector.sentPosDetect(plainText);
		for (Span span : spans) {
			span.getStart();
			span.getEnd();
			span.getType();
			DefaultTextAnnotation annot = new DefaultTextAnnotation(span.getStart(), span.getEnd());
			annot.setCoveredText(span.getCoveredText(plainText).toString());
			DefaultClassMention cm = new DefaultClassMention("sentence");
			annot.setClassMention(cm);
			annot.setAnnotator(new Annotator(null, "OpenNLP", "OpenNLP"));
			annots.add(annot);
		}

		TextDocument td = new TextDocument("12345", "unknown", plainText);
		td.addAnnotations(annots);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		writer.serialize(td, outStream, CharacterEncoding.UTF_8);
		return outStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());

	}

}
