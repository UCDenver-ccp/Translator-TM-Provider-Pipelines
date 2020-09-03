package edu.cuanschutz.ccp.tm_provider.etl.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.digest.DigestUtils;

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
	private final String entitySpan1;
	private final String entityId2;
	private final String entityCoveredText2;
	private final String entitySpan2;
	private final String keyword;
	private final String sentenceText;
	/**
	 * Larger block of text, perhaps entire abstract
	 */
	private final String sentenceContext;

	public ExtractedSentence(String documentId, String entityId1, String entityCoveredText1, String entitySpan1,
			String entityId2, String entityCoveredText2, String entitySpan2, String keyword, String sentenceText,
			String sentenceContext) {
		super();
		this.documentId = documentId;
		this.entityId1 = entityId1;
		this.entityCoveredText1 = entityCoveredText1;
		this.entitySpan1 = entitySpan1;
		this.entityId2 = entityId2;
		this.entityCoveredText2 = entityCoveredText2;
		this.entitySpan2 = entitySpan2;
		this.keyword = keyword;
		this.sentenceText = sentenceText;
		this.sentenceContext = sentenceContext.replaceAll("\\n", " ");
	}

	public String getSentenceIdentifier() {
		return DigestUtils.sha256Hex(documentId + entityId1 + entitySpan1 + entityId2 + entitySpan2 + sentenceText);
	}

	public String toTsv() {
		String blankColumn = "";
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s", getSentenceIdentifier(), documentId,
				entityCoveredText1, entityId1, entitySpan1, entityCoveredText2, entityId2, entitySpan2, keyword,
				sentenceText.length(), blankColumn, sentenceText, sentenceContext);

	}

}
