package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.file.conversion.conllu.CoNLLUDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * This function takes dependency parse CoNNL-U files and plain text files, and
 * generates a Bionlp formatted file containing sentence annotations.s
 * 
 */
public class DependencyParseConlluToSentenceFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole sentence annotation document is too large to store in Datastore. The
	 * list allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, List<String>>> SENTENCE_ANNOT_TAG = new TupleTag<KV<ProcessingStatus, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToInputText,
			DocumentCriteria dc, com.google.cloud.Timestamp timestamp) {

		return statusEntityToInputText.apply("dp_connlu->sent", ParDo.of(
				new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(
							@Element KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText,
							MultiOutputReceiver out) {
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						String documentText = PipelineMain.getDocumentText(statusEntityToText.getValue(), docId);
						String conllu = PipelineMain.getDocumentByType(statusEntityToText.getValue(),
								DocumentType.DEPENDENCY_PARSE, docId);

						try {
							String bionlp = getSentencesAsBioNLP(docId, documentText, conllu);

							/*
							 * divide the document content into chunks if necessary so that each chunk is
							 * under the DataStore byte length threshold
							 */
							List<String> chunkedConllu = PipelineMain.chunkContent(bionlp);
							out.get(SENTENCE_ANNOT_TAG).output(KV.of(statusEntity, chunkedConllu));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(dc, "Failure during dp-to-sentence.", docId, t,
									timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}
				}).withOutputTags(SENTENCE_ANNOT_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * We will make use of sentence breaks defined in the dependency parse output;
	 * converting them into BioNLP format.
	 * 
	 * @param docId
	 * @param docText
	 * @param conllu
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String getSentencesAsBioNLP(String docId, String docText, String conllu) throws IOException {

		TextDocument td = extractSentencesFromCoNLLU(docId, docText, conllu);

		BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		writer.serialize(td, outStream, CharacterEncoding.UTF_8);
		String bionlp = outStream.toString(CharacterEncoding.UTF_8.getCharacterSetName());

		return bionlp;
	}

	/**
	 * Parse the CoNLLU file and return a {@link TextDocument} that contains
	 * sentence annotations
	 * 
	 * @param docId
	 * @param docText
	 * @param conllu
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static TextDocument extractSentencesFromCoNLLU(String docId, String docText, String conllu)
			throws IOException {
		TextDocument td = new TextDocument(docId, "unknown", docText);

		CoNLLUDocumentReader conlluDocReader = new CoNLLUDocumentReader();
		TextDocument conlluDoc = conlluDocReader.readDocument(docId, "unknown",
				new ByteArrayInputStream(conllu.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				CharacterEncoding.UTF_8);

		for (TextAnnotation annot : conlluDoc.getAnnotations()) {
			if (annot.getClassMention().getMentionName().equals("sentence")) {
				// the conllu reader returns sentence annotations that are missing covered text,
				// so we add it here.s
				annot.setCoveredText(docText.substring(annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd()));
				td.addAnnotation(annot);
			}
		}

		return td;
	}

}
