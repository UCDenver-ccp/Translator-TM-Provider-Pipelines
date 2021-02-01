package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	/**
	 * Larger block of text, perhaps entire abstract
	 */
	private final String sentenceContext;

	private final String entityPlaceholder1;
	private final String entityPlaceholder2;

	public ExtractedSentence(String documentId, String entityId1, String entityCoveredText1, List<Span> entitySpan1,
			String entityPlaceholder1, String entityId2, String entityCoveredText2, List<Span> entitySpan2,
			String entityPlaceholder2, String keyword, String sentenceText, String sentenceContext) {
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
		this.keyword = keyword;
		this.sentenceText = sentenceText;
		this.sentenceContext = sentenceContext.replaceAll("\\n", "||||");
	}

	public String getSentenceIdentifier() {
		return DigestUtils.sha256Hex(documentId + entityId1 + entitySpan1 + entityId2 + entitySpan2 + sentenceText);
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
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s", getSentenceIdentifier(),
				getSentenceWithPlaceholders(), documentId, entityCoveredText1, entityId1, entitySpan1.toString(),
				entityCoveredText2, entityId2, entitySpan2.toString(), keyword, sentenceText.length(), blankColumn, sentenceText,
				sentenceContext);

	}

	private Span getAggregateSpan(List<Span> spans) {
		Collections.sort(spans, Span.ASCENDING());
		return new Span(spans.get(0).getSpanStart(), spans.get(spans.size() - 1).getSpanEnd());
	}

}
