package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.tools.ant.util.StringUtils;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.TextExtractionPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Getter;

/**
 * 
 * Consumes Strings which are serialized CONLL-U format of potentially many
 * different documents, outputs distinct chunks of CONLL-U for each document in
 * the input.
 * 
 */
public class DependencyParseImportFn extends DoFn<KV<String, String>, KV<String, String>> {
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
	public static TupleTag<KV<String, Set<String>>> DOC_ID_TO_COLLECTIONS_TAG = new TupleTag<KV<String, Set<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(PCollection<ReadableFile> files, DocumentCriteria errorDocCriteria,
			com.google.cloud.Timestamp timestamp) {

		return files.apply("Segment CONLLU into separate docs",
				ParDo.of(new DoFn<ReadableFile, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {

						ReadableFile f = context.element();
						String filename = f.getMetadata().resourceId().getFilename();

						try {
							ReadableByteChannel rbc = f.open();
							try (InputStream stream = Channels.newInputStream(rbc);) {
								for (BulkConlluIterator bulkConlluIter = new BulkConlluIterator(stream); bulkConlluIter
										.hasNext();) {
									String docId = bulkConlluIter.getDocId();
									Set<String> collections = bulkConlluIter.getCollections();
									String singleConllu = bulkConlluIter.next();

									List<String> chunkedConllu = PipelineMain.chunkContent(singleConllu);

									context.output(CONLLU_TAG, KV.of(docId, chunkedConllu));
									context.output(DOC_ID_TO_COLLECTIONS_TAG, KV.of(docId, collections));
								}
							}

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(errorDocCriteria,
									"Likely failure during CONLLU parsing.", filename, t, timestamp);
							context.output(ETL_FAILURE_TAG, failure);
						}

					}

				}).withOutputTags(CONLLU_TAG, TupleTagList.of(ETL_FAILURE_TAG).and(DOC_ID_TO_COLLECTIONS_TAG)));
	}

	protected static class BulkConlluIterator implements Iterator<String> {

		private static final String DOCUMENT_COLLECTIONS_PREFIX = "# "
				+ TextExtractionPipeline.DOCUMENT_COLLECTIONS_PREFIX_PART;
		private static final String DOCUMENT_ID_PREFIX = "# " + TextExtractionPipeline.DOCUMENT_ID_PREFIX_PART;
		/**
		 * thisDocId corresponds to the next Conll-U document that will be returned;
		 * note the document ID must be requested prior to calling next() in order for
		 * it to align with the document that is returned.
		 */
		private String thisDocId = null;
		private Set<String> thisDocCollections = null;

		@Getter
		private String nextConlluDoc = null;
		private StringBuilder nextConlluDocBuilder = null;
		private StreamLineIterator lineIter;

		public BulkConlluIterator(InputStream bulkConlluStream) throws IOException {
			lineIter = new StreamLineIterator(bulkConlluStream, CharacterEncoding.UTF_8, null);
			advanceToFirstDoc();
		}

		public String getDocId() {
			return thisDocId;
		}

		public Set<String> getCollections() {
			return thisDocCollections;
		}

		private void setNextConlluDoc(String s) {
			this.nextConlluDoc = s;
		}

		@Override
		public boolean hasNext() {
			if (getNextConlluDoc() == null) {
				return hasNextDocument();
			} else {
				return true;
			}
		}

		@Override
		public String next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			String toReturn = getNextConlluDoc();

			setNextConlluDoc(null);
			thisDocCollections = null;
			if (nextConlluDocBuilder != null) {
				updateDocIdFromCommentLine(nextConlluDocBuilder.toString());
			}
			return toReturn;
		}

		private boolean hasNextDocument() {
			if (nextConlluDocBuilder == null) {
				return false;
			}
			getRestOfDocument();
			return true;
		}

		private void getRestOfDocument() {
			while (lineIter.hasNext()) {
				Line line = lineIter.next();
				String text = line.getText();

				/*
				 * if we reach the next comment line that contains a document ID, then the
				 * current document is complete
				 */
				if (text.startsWith(DOCUMENT_ID_PREFIX)) {
					setNextConlluDoc(nextConlluDocBuilder.toString());

					System.out.println("id line: " + text);

					/* reset the string builder and add the document ID line as the first line */
					nextConlluDocBuilder = new StringBuilder();
					nextConlluDocBuilder.append(text + "\n");

					return;
				} else if (text.startsWith(DOCUMENT_COLLECTIONS_PREFIX)) {
					updateDocCollectionsFromCommentLine(text);
					nextConlluDocBuilder.append(text + "\n");

					System.out.println("collections line: " + text);
				} else {
					nextConlluDocBuilder.append(text + "\n");
				}
			}

			/*
			 * if we reach the end of the file, the set nextConlluDoc as above, but set the
			 * string builder to null to indicate that there are no more conllu documents in
			 * the file
			 */
			setNextConlluDoc(nextConlluDocBuilder.toString());
			nextConlluDocBuilder = null;
		}

		/**
		 * The first line of the document should be a comment line that contains the
		 * document ID, but we allow for blank lines just in case. If a non-blank line
		 * is observed, that does not have the document ID, then an error is reported.
		 * 
		 * The result of this method is that thisDocId is populated with the document
		 * ID, and the nextConlluDoc StringBuilder is initialized with the comment line
		 * as the first line.
		 */
		private void advanceToFirstDoc() {
			while (lineIter.hasNext()) {
				Line line = lineIter.next();
				String text = line.getText();
				if (text.trim().isEmpty()) {
					continue;
				} else {
					/*
					 * if the text is not empty, then we should have reached a comment containing
					 * the document ID of the next document. If the line contains text, but not a
					 * document ID comment, then error as this is unexpected.
					 */
					if (text.startsWith(DOCUMENT_ID_PREFIX)) {
						/* here we set the document ID for "this" document */
						updateDocIdFromCommentLine(text);
						nextConlluDocBuilder = new StringBuilder();
						nextConlluDocBuilder.append(text + "\n");
						break;
					} else {
						throw new IllegalStateException(
								"Unexpected text encountered while segmenting Conll-U file. Expected"
										+ " a comment line with the document ID, but observed the "
										+ "following instead: " + text);
					}
				}

			}
		}

		/**
		 * Parses the document ID from the specified comment line
		 * 
		 * @param docIdCommentLine
		 */
		private void updateDocIdFromCommentLine(String docIdCommentLine) {
			thisDocId = StringUtils.removePrefix(docIdCommentLine, DOCUMENT_ID_PREFIX).trim();
		}

		/**
		 * Parses the document ID from the specified comment line
		 * 
		 * @param docIdCommentLine
		 */
		private void updateDocCollectionsFromCommentLine(String commentLine) {
			String collectionsStr = StringUtils.removePrefix(commentLine, DOCUMENT_COLLECTIONS_PREFIX);
			thisDocCollections = new HashSet<String>(Arrays.asList(collectionsStr.split("\\|")));
		}

	}

}
