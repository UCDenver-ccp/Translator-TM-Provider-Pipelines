package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.annotation.impl.DefaultTextAnnotation;
import edu.ucdenver.ccp.nlp.core.mention.ClassMentionType;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;
import lombok.Data;
import lombok.EqualsAndHashCode;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * Outputs sentences in WebAnno format for sentences that have at least two
 * concept annotations, one from each of the specified types
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class SentenceExtractionWebAnnoFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	@VisibleForTesting
	protected static final String X_CONCEPTS = "X";
	@VisibleForTesting
	protected static final String Y_CONCEPTS = "Y";

	@SuppressWarnings("serial")
	public static TupleTag<KV<ProcessingStatus, String>> EXTRACTED_SENTENCES_TAG = new TupleTag<KV<ProcessingStatus, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> ETL_FAILURE_TAG = new TupleTag<EtlFailureData>() {
	};

	public static PCollectionTuple process(
			PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntityToText, Set<String> keywords,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp,
			Set<DocumentCriteria> requiredDocumentCriteria, Map<String, String> prefixToPlaceholderMap,
			DocumentType conceptDocumentType, PCollectionView<Map<String, Set<String>>> ancestorsMapView) {

		return statusEntityToText.apply("Identify concept annotations",
				ParDo.of(new DoFn<KV<ProcessingStatus, Map<DocumentCriteria, String>>, KV<ProcessingStatus, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context, MultiOutputReceiver out) {
						KV<ProcessingStatus, Map<DocumentCriteria, String>> statusEntityToText = context.element();
						SimpleTokenizer tokenizer = new SimpleTokenizer();
						ProcessingStatus statusEntity = statusEntityToText.getKey();
						String docId = statusEntity.getDocumentId();

						Map<String, Set<String>> ancestorsMap = context.sideInput(ancestorsMapView);

						try {
							String documentText = PipelineMain.getDocumentText(statusEntityToText.getValue());

							Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap = PipelineMain
									.getDocTypeToContentMap(docId, statusEntityToText.getValue());

							Set<String> extractedSentences = extractSentences(docId, documentText, docTypeToContentMap,
									keywords, prefixToPlaceholderMap, tokenizer, conceptDocumentType, ancestorsMap);
							if (extractedSentences == null) {
								PipelineMain.logFailure(ETL_FAILURE_TAG,
										"Unable to extract sentences due to missing documents for: " + docId
												+ " -- contains (" + statusEntityToText.getValue().size() + ") "
												+ statusEntityToText.getValue().keySet().toString(),
										outputDocCriteria, timestamp, out, docId, null);
							} else {
								for (String es : extractedSentences) {
									out.get(EXTRACTED_SENTENCES_TAG).output(KV.of(statusEntity, es));
								}
							}
						} catch (Throwable t) {
							PipelineMain.logFailure(ETL_FAILURE_TAG, "Failure during sentence extraction",
									outputDocCriteria, timestamp, out, docId, t);
						}
					}

				}).withSideInputs(ancestorsMapView).withOutputTags(EXTRACTED_SENTENCES_TAG,
						TupleTagList.of(ETL_FAILURE_TAG)));
	}

	/**
	 * @param documentId
	 * @param documentText
	 * @param docTypeToContentMap
	 * @param keywords
	 * @param prefixToPlaceholderMap
	 * @param tokenizer
	 * @param conceptDocumentType    CONCEPT_ALL or CONCEPT_ALL_UNFILTERED
	 * @return
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static Set<String> extractSentences(String documentId, String documentText,
			Map<DocumentType, Collection<TextAnnotation>> docTypeToContentMap, Set<String> keywords,
			Map<String, String> prefixToPlaceholderMap, SimpleTokenizer tokenizer, DocumentType conceptDocumentType,
			Map<String, Set<String>> ancestorMap) throws IOException {

		Collection<TextAnnotation> sentenceAnnots = docTypeToContentMap.get(DocumentType.SENTENCE);
		Collection<TextAnnotation> conceptAnnots = docTypeToContentMap.get(conceptDocumentType);

		String xPrefix = null;
		String yPrefix = null;
		// there are only two prefixes in the map, e.g. CHEBI, CL, MONDO.
		for (String prefix : prefixToPlaceholderMap.keySet()) {
			if (xPrefix == null) {
				xPrefix = prefix;
			} else {
				yPrefix = prefix;
			}
		}

		// if there is only one prefix - then we are looking for sentences that have two
		// of the same kind of anntotation, e.g. two protein annotations
		if (yPrefix == null) {
			yPrefix = xPrefix;
		}

		List<TextAnnotation> conceptXAnnots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots,
				Arrays.asList(xPrefix), ancestorMap);
		List<TextAnnotation> conceptYAnnots = SentenceExtractionFn.getAnnotsByPrefix(conceptAnnots,
				Arrays.asList(yPrefix), ancestorMap);

		System.out.println("CONCEPT X: " + conceptXAnnots.size());
		System.out.println("CONCEPT Y: " + conceptYAnnots.size());

		Set<String> extractedSentences = new HashSet<String>();
		if (!conceptXAnnots.isEmpty() && !conceptYAnnots.isEmpty()) {

			Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap = SentenceExtractionFn
					.buildSentenceToConceptMap(sentenceAnnots, conceptXAnnots, conceptYAnnots);

			String xPlaceholder = prefixToPlaceholderMap.get(xPrefix);
			String yPlaceholder = prefixToPlaceholderMap.get(yPrefix);

			// remove leading @ and trailing $
			if (xPlaceholder.startsWith("@") && xPlaceholder.endsWith("$")) {
				xPlaceholder = xPlaceholder.substring(1, xPlaceholder.length() - 1);
			}
			if (yPlaceholder.startsWith("@") && yPlaceholder.endsWith("$")) {
				yPlaceholder = yPlaceholder.substring(1, yPlaceholder.length() - 1);
			}

			extractedSentences.addAll(catalogExtractedSentences(keywords, documentText, documentId,
					sentenceToConceptMap, xPlaceholder, yPlaceholder, tokenizer));

		}

		System.out.println("# sentence in output: " + extractedSentences.size());
		return extractedSentences;
	}

	@VisibleForTesting
	protected static Set<String> catalogExtractedSentences(Set<String> keywords, String documentText, String documentId,
			Map<TextAnnotation, Map<String, Set<TextAnnotation>>> sentenceToConceptMap, String xPlaceholder,
			String yPlaceholder, SimpleTokenizer tokenizer) {

		Set<String> extractedSentences = new HashSet<String>();
		for (Entry<TextAnnotation, Map<String, Set<TextAnnotation>>> entry : sentenceToConceptMap.entrySet()) {
			TextAnnotation sentenceAnnot = entry.getKey();
			String keywordInSentence = SentenceExtractionFn.sentenceContainsKeyword(sentenceAnnot.getCoveredText(),
					keywords);
			if (keywords == null || keywords.isEmpty() || keywordInSentence != null) {
				if (entry.getValue().size() > 1) {
					// then this sentence contains at least 1 concept X and 1 concept Y
					Set<TextAnnotation> xConceptsInSentence = clone(documentText, entry.getValue().get(X_CONCEPTS));
					Set<TextAnnotation> yConceptsInSentence = clone(documentText, entry.getValue().get(Y_CONCEPTS));

					System.out.println("X CONCEPTS IN SENT: " + xConceptsInSentence.size());
					System.out.println("Y CONCEPTS IN SENT: " + yConceptsInSentence.size());

					// assign categories as mention names instead of ontology concept identifiers
					// offset spans so that concept spans are relative to the sentence
					for (TextAnnotation xAnnot : xConceptsInSentence) {
						List<Span> xSpan = SentenceExtractionFn.offsetSpan(xAnnot.getSpans(),
								sentenceAnnot.getAnnotationSpanStart());
						xAnnot.setSpans(xSpan);
						xAnnot.getClassMention().setMentionName(xPlaceholder);
					}
					for (TextAnnotation yAnnot : yConceptsInSentence) {
						List<Span> ySpan = SentenceExtractionFn.offsetSpan(yAnnot.getSpans(),
								sentenceAnnot.getAnnotationSpanStart());
						yAnnot.setSpans(ySpan);
						yAnnot.getClassMention().setMentionName(yPlaceholder);
					}

					Set<TextAnnotation> union = new HashSet<TextAnnotation>();
					union.addAll(yConceptsInSentence);
					union.addAll(xConceptsInSentence);

					System.out.println("XSIZE: " + union.size());
					System.out.println(union);
					// must be at least two annotations
					if (union.size() > 1) {
						String sentenceText = documentText.substring(sentenceAnnot.getAnnotationSpanStart(),
								sentenceAnnot.getAnnotationSpanEnd()) + " -- " + documentId;
						String webAnnoTsv = createWebAnnotTsv(union, sentenceText, tokenizer);
						extractedSentences.add(webAnnoTsv);
					}
				}

			}
		}
		return extractedSentences;
	}

	private static Set<TextAnnotation> clone(String documentText, Set<TextAnnotation> set) {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		Set<TextAnnotation> clones = new HashSet<TextAnnotation>();

		for (TextAnnotation annot : set) {
			List<Span> spans = new ArrayList<Span>();
			for (Span span : annot.getSpans()) {
				spans.add(new Span(span.getSpanStart(), span.getSpanEnd()));
			}
			TextAnnotation clone = factory.createAnnotation(spans, documentText,
					new DefaultClassMention(annot.getClassMention().getMentionName()));
			clones.add(clone);
		}

		return clones;
	}

	protected static String createWebAnnotTsv(Set<TextAnnotation> currentSentenceEntities, String sentence,
			SimpleTokenizer tokenizer) {

		int multiTokenEntityIndex = 1;
		int sentenceNum = 1;
		StringBuilder builder = new StringBuilder();
		builder.append("\n#Text=" + sentence + "\n");

		List<TextAnnotation> tokens = tokenizer.tokenize(sentence);

		Map<TextAnnotation, Set<TextAnnotation>> entityToOverlappingTokenMap = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		/* set token type to underscore */
		for (TextAnnotation token : tokens) {
			token.getClassMention().setMentionName("_");
		}

		for (TextAnnotation entity : currentSentenceEntities) {
			for (TextAnnotation token : tokens) {
				if (token.overlaps(entity)) {
					CollectionsUtil.addToOne2ManyUniqueMap(entity, token, entityToOverlappingTokenMap);
				}
			}
		}

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : entityToOverlappingTokenMap.entrySet()) {
			TextAnnotation entityAnnot = entry.getKey();
			Set<TextAnnotation> tokenAnnots = entry.getValue();
			String suffix = (tokenAnnots.size() > 1) ? String.format("[%d]", multiTokenEntityIndex++) : "";
			String type = entityAnnot.getClassMention().getMentionName() + suffix;
			for (TextAnnotation token : tokenAnnots) {
				String label = token.getClassMention().getMentionName();
				label = (!label.equals("_")) ? label + "|" + type : type;
				token.getClassMention().setMentionName(label);
			}
		}

		/*
		 * look for tokens that have a pipe in their label. If they do, then all parts
		 * of the label should have a number suffix, e.g. [n]
		 */
		for (TextAnnotation token : tokens) {
			String label = token.getClassMention().getMentionName();
			if (label.contains("|")) {
				StringBuilder updatedLabel = new StringBuilder();
				String[] labelParts = label.split("\\|");
				for (String labelPart : labelParts) {
					String updatedLabelPart = labelPart;
					if (!labelPart.contains("[")) {
						updatedLabelPart = labelPart + String.format("[%d]", multiTokenEntityIndex++);
					}
					if (updatedLabel.length() == 0) {
						updatedLabel.append(updatedLabelPart);
					} else {
						updatedLabel.append(String.format("|%s", updatedLabelPart));
					}
				}
			}
		}

		int tokenNum = 1;
		for (TextAnnotation token : tokens) {
			builder.append(String.format("%d-%d\t%d-%d\t%s\t%s\n", sentenceNum, tokenNum++,
					token.getAggregateSpan().getSpanStart(), token.getAggregateSpan().getSpanEnd(),
					token.getCoveredText(), token.getClassMention().getMentionName()));
		}

		return builder.toString();
	}

	/**
	 * This class also tracks and index used for multi-token annotations b/c the
	 * index needs to increment and can't start over with each sentence in the
	 * WebAnnot format (at leasst based on looking at an example).
	 *
	 */
	public static class SimpleTokenizer {

		private static final String TOKEN = ClassMentionType.TOKEN.name();
		private Tokenizer tokenizer;
//		@Getter
//		private int multiTokenIndex = 1; // for Inception, the first index is 1 not zero

		public SimpleTokenizer() {
			try (InputStream modelStream = ClassPathUtil.getResourceStreamFromClasspath(getClass(),
					"/de/tudarmstadt/ukp/dkpro/core/opennlp/lib/token-en-maxent.bin")) {
				TokenizerModel model = new TokenizerModel(modelStream);
				tokenizer = new TokenizerME(model);
			} catch (IOException e) {
				throw new IllegalStateException("Error while initializing tokenizer", e);
			}
		}

		private List<TextAnnotation> getTokensFromText(int characterOffset, String inputText) {
//			String[] tokenizedSentence = tokenizer.tokenize(inputText);
			List<TextAnnotation> annots = new ArrayList<TextAnnotation>();
			opennlp.tools.util.Span[] spans = tokenizer.tokenizePos(inputText);
			for (int i = 0; i < spans.length; i++) {
				opennlp.tools.util.Span span = spans[i];
				DefaultTextAnnotation annot = new DefaultTextAnnotation(span.getStart() + characterOffset,
						span.getEnd() + characterOffset);
				annot.setCoveredText(span.getCoveredText(inputText).toString());
				DefaultClassMention cm = new DefaultClassMention(TOKEN);
				annot.setClassMention(cm);
				annots.add(annot);
			}
			return annots;
		}

		public List<TextAnnotation> tokenize(int characterOffset, String inputText) {
			return getTokensFromText(characterOffset, inputText);
		}

		public List<TextAnnotation> tokenize(String inputText) {
			return getTokensFromText(0, inputText);
		}

//		public void incrementMultiTokenIndex() {
//			multiTokenIndex++;
//		}

	}

}
