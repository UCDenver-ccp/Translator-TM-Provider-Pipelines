package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import edu.ucdenver.ccp.file.conversion.conllu.CoNLLUDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * This function takes dependency parse CoNNL-U files generates a file
 * containing tokens in the CoNLL '03 format (single column with tokens in
 * order, sentence boundaries indicated by a blank line).
 * 
 */
public class DependencyParseConlluToConll03Fn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	/**
	 * The prefix for a line that will be used to separate documents as multiple
	 * documents will end up in a single file.
	 */
	protected static final String DOCUMENT_ID_LINE_PREFIX = "# ~!~!~! DOCUMENT_ID:";
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole sentence annotation document is too large to store in Datastore. The
	 * list allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, String>> CONLL03_TAG = new TupleTag<KV<ProcessingStatus, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToInputText,
			DocumentCriteria dc, com.google.cloud.Timestamp timestamp) {

		return statusEntityToInputText.apply("dp_connlu->conll03",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, String>>() {
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
							String conll03 = getTokensAsConll03(docId, documentText, conllu);
							out.get(CONLL03_TAG).output(KV.of(statusEntity, conll03));
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(dc, "Failure during dp-to-conll03.", docId, t,
									timestamp);
							out.get(ETL_FAILURE_TAG).output(failure);
						}
					}
				}).withOutputTags(CONLL03_TAG, TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * Returns tokens in the CoNLL '03 format
	 * 
	 * @param docId
	 * @param docText
	 * @param conllu
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String getTokensAsConll03(String docId, String docText, String conllu) throws IOException {

		CoNLLUDocumentReader conlluDocReader = new CoNLLUDocumentReader();
		TextDocument conlluDoc = conlluDocReader.readDocument(docId, "unknown",
				new ByteArrayInputStream(conllu.getBytes()), new ByteArrayInputStream(docText.getBytes()),
				CharacterEncoding.UTF_8);

		Map<TextAnnotation, List<TextAnnotation>> sentenceToTokenMap = getSentenceToTokenMap(
				conlluDoc.getAnnotations());

		List<TextAnnotation> sentenceAnnots = new ArrayList<TextAnnotation>(sentenceToTokenMap.keySet());
		Collections.sort(sentenceAnnots, TextAnnotation.BY_SPAN());

		StringBuilder sb = new StringBuilder();
		String header = String.format("%s\t%s\n", DOCUMENT_ID_LINE_PREFIX, docId);
		sb.append(header);

		for (TextAnnotation sentenceAnnot : sentenceAnnots) {
			List<TextAnnotation> tokenAnnots = sentenceToTokenMap.get(sentenceAnnot);
			Collections.sort(tokenAnnots, TextAnnotation.BY_SPAN());
			for (TextAnnotation tokenAnnot : tokenAnnots) {
				sb.append(tokenAnnot.getCoveredText() + "\n");
			}
			sb.append("\n");
		}

		return sb.toString();

	}

	/**
	 * Returns a mapping from sentence annotation to token annotation. We try to
	 * take advantage of sorted order to make the mapping more efficient.
	 * 
	 * @param annotations
	 * @return
	 */
	private static Map<TextAnnotation, List<TextAnnotation>> getSentenceToTokenMap(List<TextAnnotation> annotations) {
		Map<TextAnnotation, List<TextAnnotation>> map = new HashMap<TextAnnotation, List<TextAnnotation>>();

		Collections.sort(annotations, TextAnnotation.BY_SPAN());
		List<TextAnnotation> sentenceAnnots = new ArrayList<TextAnnotation>();

		// initialize the map, and extract sentences
		for (TextAnnotation annot : annotations) {
			String type = annot.getClassMention().getMentionName();
			if (type.equals("sentence")) {
				map.put(annot, new ArrayList<TextAnnotation>());
				sentenceAnnots.add(annot);
			}
		}

		int sentIndex = 0;
		for (TextAnnotation annot : annotations) {
			String type = annot.getClassMention().getMentionName();
			if (!type.equals("sentence")) {
				// tokens should overlap with the current sentence or the next sentence. If they
				// don't then something went wrong with the sort.
				if (!overlaps(sentIndex, sentenceAnnots, annot, map)) {
					sentIndex++;
					if (!overlaps(sentIndex, sentenceAnnots, annot, map)) {
						throw new IllegalArgumentException("Something is out of order...");
					}
				}
			}
		}

		return map;
	}

	/**
	 * returns true if the token annotation overlaps with the specified sentence
	 * annotation -- the map is updated accordingly if there is an overlap
	 * 
	 * @param sentIndex
	 * @param sentenceAnnots
	 * @param tokenAnnot
	 * @param map
	 * @return
	 */
	private static boolean overlaps(int sentIndex, List<TextAnnotation> sentenceAnnots, TextAnnotation tokenAnnot,
			Map<TextAnnotation, List<TextAnnotation>> map) {
		TextAnnotation sentenceAnnot = sentenceAnnots.get(sentIndex);
		if (tokenAnnot.overlaps(sentenceAnnot)) {
			map.get(sentenceAnnot).add(tokenAnnot);
			return true;
		}
		return false;
	}

}
