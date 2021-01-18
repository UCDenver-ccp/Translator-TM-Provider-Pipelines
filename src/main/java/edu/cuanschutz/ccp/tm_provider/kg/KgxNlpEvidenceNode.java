package edu.cuanschutz.ccp.tm_provider.kg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class KgxNlpEvidenceNode extends KgxNode {
	private final String documentCurie;
	private final String sentenceText;
	private final List<Span> subjectSpans;
	private final List<Span> relationSpans;
	private final List<Span> objectSpans;
	private final String providedBy;
	private final Double score;

	public KgxNlpEvidenceNode(String correspondingEdgeId, String name, String category, String documentCurie,
			Double score, String sentenceText, List<Span> subjectSpans, List<Span> relationSpans,
			List<Span> objectSpans, String providedBy) {
		super(getId(correspondingEdgeId, documentCurie, sentenceText, subjectSpans, relationSpans, objectSpans,
				providedBy), name, category);
		this.documentCurie = documentCurie;
		this.sentenceText = sentenceText;
		this.subjectSpans = new ArrayList<Span>(subjectSpans);
		this.relationSpans = new ArrayList<Span>(relationSpans);
		this.objectSpans = new ArrayList<Span>(objectSpans);
		this.providedBy = providedBy;
		this.score = score;
	}

	private static String getId(String edgeId, String documentId, String sentenceText, List<Span> subjectSpans,
			List<Span> relationSpans, List<Span> objectSpans, String providedBy) {
		String evidenceIdStr = String.format("%s|%s|%s|%s|%s|%s|%s", documentId, sentenceText, subjectSpans.toString(),
				relationSpans.toString(), objectSpans.toString(), edgeId, providedBy);
		return DigestUtil.getBase64Sha1Digest(evidenceIdStr);
	}

	@Override
	public String toKgxString(int columnCount) {
		List<String> values = Arrays.asList(getId(), getName(), getCategory(), getDocumentCurie(),
				Double.toString(getScore()), getSentenceText(), spansToKgxStr(subjectSpans),
				spansToKgxStr(relationSpans), spansToKgxStr(objectSpans), providedBy);
		StringBuilder s = new StringBuilder(CollectionsUtil.createDelimitedString(values, "\t"));
		for (int i = values.size(); i < columnCount; i++) {
			s.append("\t");
		}
		return s.toString();
	}

	private String spansToKgxStr(List<Span> spans) {
		if (spans == null || spans.isEmpty()) {
			return "";
		}
		StringBuffer spansStr = new StringBuffer();
		for (Span span : spans) {
			spansStr.append(String.format("start: %d, end: %d|", span.getSpanStart(), span.getSpanEnd()));
		}
		spansStr.deleteCharAt(spansStr.length() - 1);
		return spansStr.toString();
	}
}