package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Layer;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.conllu.CoNLLUDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class SentenceCooccurrenceBuilder implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Main entry point. Given a document and its annotations, return a string
	 * suitable for loading in BigQuery
	 * 
	 * @param docId
	 * @param sourceStrContainsYear
	 * @param docTypeToContent
	 * @return
	 * @throws IOException
	 */
	public String toSentenceCooccurrenceString(String docId, Map<DocumentType, String> docTypeToContent)
			throws IOException {

		Set<String> stopwords = new HashSet<String>(FileReaderUtil.loadLinesFromFile(
				getClass().getClassLoader().getResourceAsStream("oger/stopwords/stopwords-long.txt"),
				CharacterEncoding.UTF_8));

		TextDocument td = extractAllAnnotations(docId, docTypeToContent);

		StringBuilder builder = new StringBuilder();


		Map<Span, TextAnnotation> spanToSentenceAnnotMap = new HashMap<Span, TextAnnotation>();

		for (TextAnnotation ta : td.getAnnotations()) {
			if (ta.getClassMention().getMentionName().equalsIgnoreCase("sentence")) {
				String annotationId = BigQueryUtil.getAnnotationIdentifier(docId, ta.getAggregateSpan().getSpanStart(),
						ta.getCoveredText(), Layer.SENTENCE, "sentence");
				ta.setAnnotationID(annotationId);
				spanToSentenceAnnotMap.put(ta.getAggregateSpan(), ta);

//				// check to make sure spans are aligned with original text
//				String coveredText = ta.getCoveredText();
//				String expectedCoveredText = td.getText().substring(ta.getAnnotationSpanStart(),
//						ta.getAnnotationSpanEnd());
//				if (!coveredText.equals(expectedCoveredText)) {
//					throw new RuntimeException("sentence text not as expected");
//				}

			}
		}

		Map<Span, TextAnnotation> sortedSpanToSentenceMap = CollectionsUtil.sortMapByKeys(spanToSentenceAnnotMap,
				SortOrder.ASCENDING);
		builder.append(String.format("SENT_COUNT\t%d\t%s\n", sortedSpanToSentenceMap.size(), docId));

		Map<TextAnnotation, Set<TextAnnotation>> sentenceIdToConceptAnnotMap = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		for (TextAnnotation ta : td.getAnnotations()) {
			if (!ta.getClassMention().getMentionName().equalsIgnoreCase("sentence")) {

//				// check to make sure spans are aligned with original text
				String coveredText = ta.getCoveredText();
//				String expectedCoveredText = td.getText().substring(ta.getAnnotationSpanStart(),
//						ta.getAnnotationSpanEnd());
//				if (!coveredText.equals(expectedCoveredText)) {
//					System.out.println("CT : " + coveredText);
//					System.out.println("ECT: " + expectedCoveredText);
//
//					throw new RuntimeException("sentence text not as expected");
//				}

				for (Entry<Span, TextAnnotation> entry : sortedSpanToSentenceMap.entrySet()) {
					if (entry.getKey().overlaps(ta.getAggregateSpan())) {
//						String coveredText = ta.getCoveredText();
						if (!stopwords.contains(coveredText.toLowerCase())) {
							CollectionsUtil.addToOne2ManyUniqueMap(entry.getValue(), ta, sentenceIdToConceptAnnotMap);
						}
//						else {
//							System.out.println("remove stopword: " + coveredText);
//						}
					}
				}
			}
		}

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : sentenceIdToConceptAnnotMap.entrySet()) {
			TextAnnotation sentenceAnnot = entry.getKey();
			int offset = sentenceAnnot.getAnnotationSpanStart();
			builder.append(sentenceAnnot.getAnnotationID() + "\t");
			for (TextAnnotation conceptAnnot : entry.getValue()) {
				builder.append((conceptAnnot.getAnnotationSpanStart() - offset) + "|"
						+ (conceptAnnot.getAnnotationSpanEnd() - offset) + "|"
						+ conceptAnnot.getClassMention().getMentionName() + ";");

				// check to make sure spans are aligned with original text
//				String coveredText = conceptAnnot.getCoveredText();

//				System.out.println("COVERED TEXT: " + coveredText);

//				String expectedCoveredText = td.getText().substring(conceptAnnot.getAnnotationSpanStart(),
//						conceptAnnot.getAnnotationSpanEnd());
//				if (!coveredText.equals(expectedCoveredText)) {
//					throw new RuntimeException("concept text not as expected");
//				}

//				coveredText = sentenceAnnot.getCoveredText();
//				expectedCoveredText = td.getText().substring(sentenceAnnot.getAnnotationSpanStart(),
//						sentenceAnnot.getAnnotationSpanEnd());
//				if (!coveredText.equals(expectedCoveredText)) {
//					throw new RuntimeException("sentence text not as expected");
//				}

//				coveredText = conceptAnnot.getCoveredText();

//				System.out.println("Covered text: " + coveredText + " span: " + ta.getAggregateSpan().toString());
//				System.out.println("sentence: " + entry.getKey().getCoveredText() + " span: "
//						+ entry.getKey().getAggregateSpan().toString());

//				String expectedCoveredTextInDocument = td.getText().substring(ta.getAnnotationSpanStart(),
//						ta.getAnnotationSpanEnd());

//				if (conceptAnnot.getAnnotationSpanEnd() - offset > sentenceAnnot.getCoveredText().length()) {
//					throw new RuntimeException(
//							"MISMATCH: concept span (adjusted): " + (conceptAnnot.getAnnotationSpanStart() - offset)
//									+ " .. " + (conceptAnnot.getAnnotationSpanEnd() - offset) + "\nSentence length: "
//									+ sentenceAnnot.getCoveredText().length() + "\n" + conceptAnnot.toString() + "\n"
//									+ sentenceAnnot.toString());
//				}

//				String expectedCoveredTextInSentence = sentenceAnnot.getCoveredText().substring(
//						conceptAnnot.getAnnotationSpanStart() - offset, conceptAnnot.getAnnotationSpanEnd() - offset);
//				if (!coveredText.equals(expectedCoveredTextInSentence)) {
//					throw new RuntimeException("covered text does not match!!!");
//				}

			}
			builder.append("\t" + entry.getKey().getCoveredText().replaceAll("\\n", " "));
			builder.append("\n");

		}

		return builder.toString();
	}

	/**
	 * Extract all annotations from the annotation files that have accompanied the
	 * document. Some assumptions are made regarding annotator names to assign.
	 * 
	 * @param docId
	 * @param sourceStrContainsYear
	 * @param docTypeToContent
	 * @return
	 * @throws IOException
	 */
	static TextDocument extractAllAnnotations(String docId, Map<DocumentType, String> docTypeToContent)
			throws IOException {
		String text = docTypeToContent.get(DocumentType.TEXT);
		TextDocument td = new TextDocument(docId, null, text);

		for (Entry<DocumentType, String> entry : docTypeToContent.entrySet()) {
			// get sentences - prefer the dependency parse sentences if both are available
			if (entry.getKey() == DocumentType.DEPENDENCY_PARSE) {
				CoNLLUDocumentReader reader = new CoNLLUDocumentReader();
				TextDocument doc = reader.readDocument(docId, "unknown-source",
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);
				for (TextAnnotation ta : doc.getAnnotations()) {
					if (ta.getClassMention().getMentionName().equalsIgnoreCase("sentence")) {
						ta.setAnnotator(new Annotator(null, "turku", null));
						ta.setCoveredText(text.substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd()));
						td.addAnnotation(ta);

					}
				}
				// get sentences - prefer the dependency parse sentences if both are available
			} else if (entry.getKey() == DocumentType.SENTENCE
					&& !docTypeToContent.containsKey(DocumentType.DEPENDENCY_PARSE)) {
				BioNLPDocumentReader reader = new BioNLPDocumentReader();
				TextDocument doc = reader.readDocument(docId, "unknown-source",
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);

				for (TextAnnotation ta : doc.getAnnotations()) {
					ta.setAnnotator(new Annotator(null, "OpenNLP", null));
				}

				td.addAnnotations(doc.getAnnotations());
			} else if (entry.getKey().name().startsWith("CONCEPT_")) {

//				System.out.println("++++++++++++++++ PROCESSING " + entry.getKey().name());
				BioNLPDocumentReader reader = new BioNLPDocumentReader();
				TextDocument doc = reader.readDocument(docId, "unknown-source",
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);

				// TODO: revisit this -- filter out EXT annotations -- or map them to their
				// corresponding closest canonical OBO annotation
				List<TextAnnotation> keep = new ArrayList<TextAnnotation>();
				for (TextAnnotation ta : doc.getAnnotations()) {

					// the OGER annotation offsets are byte offsets, not character position offsets,
					// so we convert them here.
					convertFromByteToCharOffset(ta, text);

					ta.setAnnotator(new Annotator(null, "oger", null));
					// exclude if contains EXT and if the part after the colon is not numbers,
					// otherwise, remove the _EXT and keep. Many of the PR and SO concepts follow
					// this pattern
					if (!ta.getClassMention().getMentionName().contains("EXT")) {
						keep.add(ta);

//						System.out.println("keep: " + ta.getClassMention().getMentionName());

					} else {
						String[] toks = ta.getClassMention().getMentionName().split(":");
						if (toks[1].matches("\\d+")) {
							String updatedMentionName = ta.getClassMention().getMentionName().replace("_EXT", "");
							ta.getClassMention().setMentionName(updatedMentionName);
							keep.add(ta);

//							System.out.println("keep: " + ta.getClassMention().getMentionName());
						}
					}
				}

				td.addAnnotations(keep);
			}
		}

		return td;
	}

	public static void convertFromByteToCharOffset(TextAnnotation ta, String text) {

		// sometimes the conversion is needed, sometimes it doesn't appear to be needed
		// -- need to look into this much more thoroughly

		String coveredText = ta.getCoveredText();

		// if the covered text from the bionlp file matches the text.substring using the
		// indexes as is, then keep them, otherwise update them
		if (ta.getAnnotationSpanEnd() >= text.length() || !coveredText.equals(text.substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd()))) {

			int updatedSpanStart = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanStart()))
					.length();
			int updatedSpanEnd = new String(Arrays.copyOfRange(text.getBytes(), 0, ta.getAnnotationSpanEnd())).length();

			ta.setAnnotationSpanStart(updatedSpanStart);
			ta.setAnnotationSpanEnd(updatedSpanEnd);

			ta.setCoveredText(text.substring(updatedSpanStart, updatedSpanEnd));
		}

	}

}
