package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.digest.DigestUtils;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@SuppressWarnings("rawtypes")
@EqualsAndHashCode(callSuper = false)
public class ExtractedSentence extends DoFn {

	private static final long serialVersionUID = 1L;

	private final String documentId;
	private final String entityId1;
	private final String entityCoveredText1;
	private final List<Span> entitySpan1;
	private final String entityId2;
	private final String entityCoveredText2;
	private final List<Span> entitySpan2;
	private final String keyword;
	private final String sentenceText;
	private final String documentZone;
	private final Set<String> documentPublicationTypes;
	private final int documentYearPublished;

//	/**
//	 * Larger block of text, perhaps entire abstract
//	 */
//	private final String sentenceContext;

	private final String entityPlaceholder1;
	private final String entityPlaceholder2;

	public ExtractedSentence(String documentId, String entityId1, String entityCoveredText1, List<Span> entitySpan1,
			String entityPlaceholder1, String entityId2, String entityCoveredText2, List<Span> entitySpan2,
//			String entityPlaceholder2, String keyword, String sentenceText, String sentenceContext, String documentZone,
			String entityPlaceholder2, String keyword, String sentenceText, String documentZone,
			Set<String> documentPublicationTypes, int documentYearPublished) {
		super();

		// order entities by span so that their order is reproducible
		Span aggregateSpan1 = getAggregateSpan(entitySpan1);
		Span aggregateSpan2 = getAggregateSpan(entitySpan2);

		if (aggregateSpan1.startsBefore(aggregateSpan2)) {
			this.entityId1 = entityId1;
			this.entityCoveredText1 = entityCoveredText1;
			this.entitySpan1 = entitySpan1;
			this.entityPlaceholder1 = entityPlaceholder1;
			this.entityId2 = entityId2;
			this.entityCoveredText2 = entityCoveredText2;
			this.entitySpan2 = entitySpan2;
			this.entityPlaceholder2 = entityPlaceholder2;
		} else {
			this.entityId1 = entityId2;
			this.entityCoveredText1 = entityCoveredText2;
			this.entitySpan1 = entitySpan2;
			this.entityPlaceholder1 = entityPlaceholder2;
			this.entityId2 = entityId1;
			this.entityCoveredText2 = entityCoveredText1;
			this.entitySpan2 = entitySpan1;
			this.entityPlaceholder2 = entityPlaceholder1;
		}

		this.documentId = documentId;
		this.keyword = (keyword != null) ? keyword : "";
		this.sentenceText = sentenceText;
//		this.sentenceContext = sentenceContext.replaceAll("\\n", "||||");
		this.documentZone = (documentZone != null) ? documentZone : "unknown";
		this.documentPublicationTypes = (documentPublicationTypes != null)
				? new HashSet<String>(documentPublicationTypes)
				: new HashSet<String>();
		this.documentYearPublished = documentYearPublished;
	}

	public String getSentenceIdentifier() {
		return DigestUtils.sha256Hex(
				documentId + documentZone + entityId1 + entitySpan1 + entityId2 + entitySpan2 + sentenceText);
	}

	public String getSentenceWithPlaceholders() {

		// if there are multiple spans for an annotation, we simply take the full
		// (aggregate) span
		Span entity1AggregateSpan = getAggregateSpan(entitySpan1);
		Span entity2AggregateSpan = getAggregateSpan(entitySpan2);

		Map<Span, String> spanToPlaceholder = new HashMap<Span, String>();
		spanToPlaceholder.put(entity1AggregateSpan, entityPlaceholder1);
		spanToPlaceholder.put(entity2AggregateSpan, entityPlaceholder2);

		Map<Span, String> sortedSpans = CollectionsUtil.sortMapByKeys(spanToPlaceholder, SortOrder.DESCENDING);

		// there should be only two spans in the map, ordered closest to the end of the
		// sentence first then closest to the beginning
		Span firstSpan = null;
		Span secondSpan = null;
		for (Entry<Span, String> entry : sortedSpans.entrySet()) {
			if (secondSpan == null) {
				secondSpan = entry.getKey();
			} else {
				firstSpan = entry.getKey();
			}
		}

		String sentenceEnd = sentenceText.substring(secondSpan.getSpanEnd());
		String sentenceMiddle = sentenceText.substring(firstSpan.getSpanEnd(), secondSpan.getSpanStart());
		String sentenceBegin = sentenceText.substring(0, firstSpan.getSpanStart());

		return sentenceBegin + sortedSpans.get(firstSpan) + sentenceMiddle + sortedSpans.get(secondSpan) + sentenceEnd;

	}

	/**
	 * @return
	 */
	public String toTsv() {
		// if the entities overlap, then return null

		if (getAggregateSpan(entitySpan1).overlaps(getAggregateSpan(entitySpan2))) {
			return null;
		}
		String blankColumn = "";
		String pubTypesStr = "";
		if (documentPublicationTypes != null) {
			pubTypesStr = CollectionsUtil.createDelimitedString(documentPublicationTypes, "|");
		}

		try {

			return CollectionsUtil.createDelimitedString(Arrays.asList(getSentenceIdentifier(),
					getSentenceWithPlaceholders(), documentId, entityCoveredText1, entityId1, getSpanStr(entitySpan1),
					entityCoveredText2, entityId2, getSpanStr(entitySpan2), keyword, sentenceText.length(), blankColumn,
//					sentenceText, documentZone, pubTypesStr, documentYearPublished, sentenceContext), "\t");
					sentenceText, documentZone, pubTypesStr, documentYearPublished), "\t");

		} catch (NullPointerException e) {
			StringBuilder msgBuilder = new StringBuilder();
			msgBuilder.append("sentence id: " + Arrays.asList(getSentenceIdentifier()) + "\n");
			msgBuilder.append("sentence with placeholders: " + getSentenceWithPlaceholders() + "\n");
			msgBuilder.append("document id: " + documentId + "\n");
			msgBuilder.append("entity covered text 1: " + entityCoveredText1 + "\n");
			msgBuilder.append("entity id 1: " + entityId1 + "\n");
			msgBuilder.append("entity span 1: " + getSpanStr(entitySpan1) + "\n");
			msgBuilder.append("entity covered text 2: " + entityCoveredText2 + "\n");
			msgBuilder.append("entity id 2: " + entityId2 + "\n");
			msgBuilder.append("entity span 2: " + getSpanStr(entitySpan2) + "\n");
			msgBuilder.append("keyword: " + keyword + "\n");
			msgBuilder.append("sentence length: " + sentenceText.length() + "\n");
			msgBuilder.append("sentence text: " + sentenceText + "\n");
			msgBuilder.append("document zone: " + documentZone + "\n");
			msgBuilder.append("pub types: " + pubTypesStr + "\n");
			msgBuilder.append("year pyblished: " + documentYearPublished + "\n");
//			msgBuilder.append("sentence context: " + sentenceContext + "\n");
			throw new NullPointerException(msgBuilder.toString());
		}

	}

	/**
	 * @param spans
	 * @return a string representation of the spans using the following format:
	 *         0|5;11|14;22|25
	 */
	public static String getSpanStr(List<Span> spans) {
		StringBuffer sb = new StringBuffer();
		sb.append(spans.get(0).getSpanStart() + "|" + spans.get(0).getSpanEnd());
		for (int i = 1; i < spans.size(); i++) {
			sb.append(";" + spans.get(i).getSpanStart() + "|" + spans.get(i).getSpanEnd());
		}
		return sb.toString();
	}

	public static ExtractedSentence fromTsv(String tsv, boolean sentenceIdInFirstColumn) {
		Pattern placeholderPattern = Pattern.compile("(@.*?\\$)"); // e.g. @GENE$

		String[] cols = tsv.split("\\t");

		int index = 0;
		if (sentenceIdInFirstColumn) {
			index++; // skip sentence identifier column
		}
		String sentenceWithPlaceholders = cols[index++];
		String documentId = cols[index++];
		String entityCoveredText1 = cols[index++];
		String entityId1 = cols[index++];
		List<Span> entitySpan1 = getSpans(cols[index++]);
		String entityCoveredText2 = cols[index++];
		String entityId2 = cols[index++];
		List<Span> entitySpan2 = getSpans(cols[index++]);
		String keyword = cols[index++];
		index++; // skip sentence length column
		index++; // skip blank column
		String sentenceText = cols[index++];
		String documentZone = cols[index++];
		Set<String> documentPublicationTypes = new HashSet<String>(Arrays.asList(cols[index++].split("\\|")));
		int documentYearPublished = Integer.parseInt(cols[index++]);
//		String sentenceContext = cols[index++];

		int entity1Start = entitySpan1.get(0).getSpanStart();
		int entity2Start = entitySpan2.get(0).getSpanStart();

		String entityPlaceholder1 = null;
		String entityPlaceholder2 = null;

		Matcher m = placeholderPattern.matcher(sentenceWithPlaceholders);
		boolean foundFirstPlaceholder = false;
		int count = 0;
		while (m.find()) {
			if (count++ > 1) {
				throw new IllegalStateException(
						"Encountered more that 2 placeholders in sentence: " + sentenceWithPlaceholders);
			}
			String placeholder = m.group(1);
			if (!foundFirstPlaceholder) {
				if (entity1Start < entity2Start) {
					entityPlaceholder1 = placeholder;
				} else {
					entityPlaceholder2 = placeholder;
				}
				foundFirstPlaceholder = true;
			} else {
				if (entityPlaceholder1 == null) {
					entityPlaceholder1 = placeholder;
				} else if (entityPlaceholder2 == null) {
					entityPlaceholder2 = placeholder;
				} else {
					throw new IllegalStateException("both should not be non-null at this point");
				}
			}

		}

		return new ExtractedSentence(documentId, entityId1, entityCoveredText1, entitySpan1, entityPlaceholder1,
				entityId2, entityCoveredText2, entitySpan2, entityPlaceholder2, keyword, sentenceText, // sentenceContext,
				documentZone, documentPublicationTypes, documentYearPublished);
	}

	protected static List<Span> getSpans(String spanStr) {
		List<Span> spans = new ArrayList<Span>();
		Pattern p = Pattern.compile("(\\d+)\\|(\\d+);?");
		Matcher m = p.matcher(spanStr);
		while (m.find()) {
			spans.add(new Span(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))));
		}

		return spans;
	}

//	/**
//	 * Parse strings like [[106..118]] and return a list of {@link Span}
//	 * 
//	 * @param spanStr
//	 * @return
//	 */
//	protected static List<Span> getSpans(String spanStr) {
//		Pattern p = Pattern.compile("\\[(\\d+)\\.\\.(\\d+)\\]");
//		List<Span> spans = new ArrayList<Span>();
//		String cleanSpanStr = spanStr;
//		// drop first and last square brackets
//		if (cleanSpanStr.startsWith("[[")) {
//			cleanSpanStr = cleanSpanStr.substring(1);
//		} else {
//			throw new IllegalArgumentException(
//					"Unexpected span serialization format: " + spanStr + ". Expected something like: [[0..5]]");
//		}
//		if (cleanSpanStr.endsWith("]]")) {
//			cleanSpanStr = cleanSpanStr.substring(0, cleanSpanStr.length() - 1);
//		} else {
//			throw new IllegalArgumentException(
//					"Unexpected span serialization format: " + spanStr + ". Expected something like: [[0..5]]");
//		}
//		// parse the inner content and create spans
//		String[] spanToks = cleanSpanStr.split(",");
//		for (String spanTok : spanToks) {
//			Matcher m = p.matcher(spanTok);
//			if (m.find()) {
//				int spanStart = Integer.parseInt(m.group(1));
//				int spanEnd = Integer.parseInt(m.group(2));
//				spans.add(new Span(spanStart, spanEnd));
//			} else {
//				throw new IllegalArgumentException(
//						"Unexpected span serialization format: " + spanTok + ". Expected something like: [0..5]");
//			}
//		}
//
//		return spans;
//	}

	private Span getAggregateSpan(List<Span> spans) {
		Collections.sort(spans, Span.ASCENDING());
		return new Span(spans.get(0).getSpanStart(), spans.get(spans.size() - 1).getSpanEnd());
	}

}
