package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Layer;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class BigQueryUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static String getAnnotationIdentifier(TextAnnotation ta, Layer annotationLayer) {
		return getAnnotationIdentifier(ta.getDocumentID(), ta.getAggregateSpan().getSpanStart(), ta.getCoveredText(),
				annotationLayer, ta.getClassMention().getMentionName());
	}

	public static String getAnnotationIdentifier(String documentId, int spanStart, String coveredText,
			Layer annotationLayer, String annotationType) {
		return getAnnotationIdentifier(documentId, spanStart, coveredText, annotationLayer,
				CollectionsUtil.createList(annotationType));
	}

	public static String getAnnotationIdentifier(String documentId, int spanStart, String coveredText,
			Layer annotationLayer, List<String> annotationTypes) {
		coveredText = coveredText.replaceAll("\\n", " ");
		coveredText = coveredText.replaceAll("\\t", " ");
		Collections.sort(annotationTypes);
		return DigestUtils.sha256Hex(documentId + spanStart + coveredText + annotationLayer.name()
				+ annotationTypes.toString().toLowerCase());
	}

}
