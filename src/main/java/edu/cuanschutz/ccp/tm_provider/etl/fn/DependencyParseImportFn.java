package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
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
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;

/**
 * 
 * Consumes Strings which are serialized CONLL-U format of potentially many
 * different documents, outputs distinct chunks for each document in the input
 * as well as sentence annotations in BioNLP format for each document.
 * 
 */
public class DependencyParseImportFn extends DoFn<KV<String, String>, KV<String, String>> {
//	private static final Logger logger = org.apache.log4j.Logger.getLogger(BiocToTextFn.class);
	private static final long serialVersionUID = 1L;
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole document content string is too large to store in Datastore. The list
	 * allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> CONLLU_TAG = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> SENTENCE_TAG = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};
//	@SuppressWarnings("serial")
//	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
//	};

	public static PCollectionTuple process(PCollection<KV<String, String>> fileIdToConllu,
			DocumentCriteria errorDocCriteria, com.google.cloud.Timestamp timestamp) {

		return fileIdToConllu.apply("Segment CONLLU into separate docs",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {

						KV<String, String> docIdToConllu = context.element();

						String fileId = docIdToConllu.getKey();
						String conllu = docIdToConllu.getValue();

						try {
							for (BulkConlluIterator bulkConlluIter = new BulkConlluIterator(conllu); bulkConlluIter
									.hasNext();) {
								String docId = bulkConlluIter.getNextDocId();
								String singleConllu = bulkConlluIter.next();
								String documentText = null;

								String sentenceBionlp = getSentenceBionlp(singleConllu, documentText);

								List<String> chunkedConllu = PipelineMain.chunkContent(singleConllu);
								List<String> chunkedSentenceBionlp = PipelineMain.chunkContent(sentenceBionlp);

								context.output(CONLLU_TAG, KV.of(docId, chunkedConllu));
								context.output(SENTENCE_TAG, KV.of(docId, chunkedSentenceBionlp));
							}

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(errorDocCriteria,
									"Likely failure during CONLLU parsing.", fileId, t, timestamp);
							context.output(ETL_FAILURE_TAG, failure);
						}

					}

					private String getSentenceBionlp(String singleConllu, String documentText) throws IOException {
						TextDocument td = new TextDocument("1234", "unknown", documentText);

						// TODO: add sentence annotations here - Use ConlluParser????

						/* serialize the annotations into the BioNLP format */
						BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						bionlpWriter.serialize(td, baos, CharacterEncoding.UTF_8);
						String serializedAnnotations = baos.toString(CharacterEncoding.UTF_8.getCharacterSetName());
						return serializedAnnotations;
					}
				}).withOutputTags(CONLLU_TAG, TupleTagList.of(SENTENCE_TAG).and(ETL_FAILURE_TAG)));
	}

	private static class BulkConlluIterator implements Iterator<String> {

		public BulkConlluIterator(String bulkConllu) {

		}

		public String getNextDocId() {
			// TODO
			return null;
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String next() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	code up TODOs in this class
	
}
