package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.SpanUtils;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import lombok.Data;
import lombok.Getter;

public class BigQueryAnnotationSerializer implements Serializable {

	private static final long serialVersionUID = 1L;

	public enum TableKey {
		ANNOTATION, IN_SENTENCE, IN_PARAGRAPH, IN_SECTION, IN_CONCEPT, RELATION
	}

	public enum Layer {
		SECTION(true), PARAGRAPH(true), SENTENCE(true), CONCEPT(true), TOKEN(false);

		@Getter
		private final boolean firstPass;

		private Layer(boolean firstPass) {
			this.firstPass = firstPass;
		}
	}

	/*
	 * The four maps below use Set<String> in the values position. This is mainly
	 * for convenience. Only the conceptSpanToIdMap map will ever have multiple
	 * entries in the Set (this will occur when there are concept annotations for
	 * parent concept classes, or potential redundant concept annotations from two
	 * different ontologies).
	 * 
	 * The convenience comes from methods that operate on all of the maps. The code
	 * is DRYer if all maps can have the same signature.
	 */

	private Map<Span, Set<String>> sectionSpanToIdMap;
	private Map<Span, Set<String>> paragraphSpanToIdMap;
	private Map<Span, Set<String>> sentenceSpanToIdMap;
	private Map<Span, Set<String>> conceptSpanToIdMap;

	private final String documentId;
//	private final Calendar publicationDate;
//	private final Integer publicationYear;
	private Map<String, Set<Relation>> annotationIdToRelationMap;
//	private Map<String, Set<String>> sectionIdToNormalizedTypeMap;

	public BigQueryAnnotationSerializer(String documentId, // Integer publicationYear,
			Map<Span, Set<String>> sectionSpanToIdMap, Map<Span, Set<String>> paragraphSpanToIdMap,
			Map<Span, Set<String>> sentenceSpanToIdMap, Map<Span, Set<String>> inputConceptSpanToIdMap,
//			Map<String, Set<String>> sectionIdToNormalizedTypeMap,
			Map<String, Set<Relation>> annotationIdToRelationMap) {
		this.documentId = documentId;
//		this.publicationDate = publicationDate;
//		this.publicationYear = publicationYear;
//		this.sectionIdToNormalizedTypeMap = sectionIdToNormalizedTypeMap;
		this.annotationIdToRelationMap = annotationIdToRelationMap;
		this.sectionSpanToIdMap = CollectionsUtil.sortMapByKeys(sectionSpanToIdMap, SortOrder.ASCENDING);
		this.paragraphSpanToIdMap = CollectionsUtil.sortMapByKeys(paragraphSpanToIdMap, SortOrder.ASCENDING);
		this.sentenceSpanToIdMap = CollectionsUtil.sortMapByKeys(sentenceSpanToIdMap, SortOrder.ASCENDING);
		this.conceptSpanToIdMap = CollectionsUtil.sortMapByKeys(inputConceptSpanToIdMap, SortOrder.ASCENDING);
		
		
	}

	public Map<TableKey, Set<String>> toString(TextAnnotation ta, String documentText) {
		Map<TableKey, Set<String>> tableKeyToAnnotLinesMap = new HashMap<TableKey, Set<String>>();
		Layer layer = determineLayer(ta);
		List<String> types = CollectionsUtil.createList(ta.getClassMention().getMentionName());
		String coveredText = SpanUtils.getCoveredText(ta.getSpans(), documentText);
		String annotationId = BigQueryUtil.getAnnotationIdentifier(documentId, ta.getAggregateSpan().getSpanStart(),
				coveredText, layer, types);
		String annotatorName = ta.getAnnotator().getName();

		List<String> sectionIds = getCoveringSpanIds(annotationId, ta.getSpans(), layer, sectionSpanToIdMap,
				Layer.SECTION);
		List<String> paragraphIds = getCoveringSpanIds(annotationId, ta.getSpans(), layer, paragraphSpanToIdMap,
				Layer.PARAGRAPH);
		List<String> sentenceIds = getCoveringSpanIds(annotationId, ta.getSpans(), layer, sentenceSpanToIdMap,
				Layer.SENTENCE);
		List<String> conceptIds = getCoveringSpanIds(annotationId, ta.getSpans(), layer, conceptSpanToIdMap,
				Layer.CONCEPT);

		for (String sectionId : sectionIds) {
			if (sectionId != null) {
				CollectionsUtil.addToOne2ManyUniqueMap(TableKey.IN_SECTION, annotationId + "\t" + sectionId,
						tableKeyToAnnotLinesMap);
			}
		}

		for (String paragraphId : paragraphIds) {
			// there should only ever be one paragraph ID covering a given
			// annotation
			if (paragraphId != null) {
				CollectionsUtil.addToOne2ManyUniqueMap(TableKey.IN_PARAGRAPH, annotationId + "\t" + paragraphId,
						tableKeyToAnnotLinesMap);
			}
		}
		for (String sentenceId : sentenceIds) {
			// there should only ever be one sentence ID covering a
			// given annotation
			if (sentenceId != null) {
				CollectionsUtil.addToOne2ManyUniqueMap(TableKey.IN_SENTENCE, annotationId + "\t" + sentenceId,
						tableKeyToAnnotLinesMap);
			}
		}
		for (String conceptId : conceptIds) {
			if (conceptId != null) {
				CollectionsUtil.addToOne2ManyUniqueMap(TableKey.IN_CONCEPT, annotationId + "\t" + conceptId,
						tableKeyToAnnotLinesMap);
			}
		}

		for (Span span : ta.getSpans()) {
			for (String type : types) {
				String annotLine = serializeAnnotation(annotationId, annotatorName, layer, type, coveredText,
						span.getSpanStart(), span.getSpanEnd());
				if (annotLine != null) {
					CollectionsUtil.addToOne2ManyUniqueMap(TableKey.ANNOTATION, annotLine, tableKeyToAnnotLinesMap);
				}
			}
		}

		if (annotationIdToRelationMap != null && annotationIdToRelationMap.containsKey(annotationId)) {
			Set<Relation> relationsList = annotationIdToRelationMap.get(annotationId);

			for (Relation relation : relationsList) {
				String relationLine = annotationId + "\t" + relation.getSource() + "\t" + relation.getType() + "\t"
						+ relation.getRelatedAnnotationId();
				CollectionsUtil.addToOne2ManyUniqueMap(TableKey.RELATION, relationLine, tableKeyToAnnotLinesMap);

			}
		}

		return tableKeyToAnnotLinesMap;
	}

	public static final List<Layer> layerOrdering = CollectionsUtil.createList(Layer.SECTION, Layer.PARAGRAPH,
			Layer.SENTENCE, Layer.CONCEPT, Layer.TOKEN);

//	private static final Set<String> EXPECTED_SECTION_TYPES = CollectionsUtil.createSet("ABBR", "ABSTRACT", "ACK_FUND",
//			"APPENDIX", "AUTH_CONT", "CASE", "COMP_INT", "CONCL", "DISCUSS", "FIG", "INTRO", "KEYWORDS", "METHODS",
//			"REF", "RESULTS", "REVIEW_INFO", "SUPPL", "TABLE", "TITLE");

	static Layer determineLayer(TextAnnotation ta) {
		String annotatorName = ta.getAnnotator().getName();
		String type = ta.getClassMention().getMentionName();

		if (annotatorName.equalsIgnoreCase("turku")) {
			if (type.equalsIgnoreCase("sentence")) {
				return Layer.SENTENCE;
			} else {
				return Layer.TOKEN;
			}
		} else if (annotatorName.equalsIgnoreCase("oger")) {
			return Layer.CONCEPT;
		} else if (annotatorName.equalsIgnoreCase("bioc")) {
			if (type.equalsIgnoreCase("paragraph")) {
				return Layer.PARAGRAPH;
			} else if (type.equalsIgnoreCase("reference")) {
				return Layer.SENTENCE;
			} else {
				return Layer.SECTION;
			}
		}

		throw new IllegalStateException("unhandled annotator/type combination: " + annotatorName + " -- " + type);

	}

	private List<String> getCoveringSpanIds(String annotationId, List<Span> annotationSpans, Layer annotationLayer,
			Map<Span, Set<String>> spanToIdMap, Layer mapLayer) {
		List<String> identifiers = new ArrayList<String>();

		/*
		 * using the ordering specified in layerOrdering, only permit an annotation to
		 * be covered by a span if its index in the layerOrdering list is greater than
		 * the index of the layer of the covering annotation. This will prevent, for
		 * example, a section from being 'inside' a sentence (which could occur b/c a
		 * section title and sentence likely have the same span).
		 */
		if (layerOrdering.indexOf(annotationLayer) >= layerOrdering.indexOf(mapLayer)) {
			/*
			 * spanToIdMap is sorted by span (ascending) so we can break once we encounter a
			 * span that is past the start of the input span(s)
			 */
			for (Entry<Span, Set<String>> entry : spanToIdMap.entrySet()) {
				boolean exitNow = true;
				for (Span span : annotationSpans) {

					// System.out.println();
					// System.out.println("LAYER=" + annotationLayer + "/" +
					// mapLayer + " Is " + span.toString() + " encompassed by "
					// + entry.getKey().toString() + "? "
					// + (entry.getKey().containsSpan(span)) + " -- " +
					// entry.getValue());

					if (entry.getKey().containsSpan(span)) {
						for (String id : entry.getValue()) {
							if (!id.equals(annotationId)) {
								identifiers.add(id);
							}
						}
						exitNow = false;
					} else if (entry.getKey().getSpanEnd() <= span.getSpanStart()) {
						// System.out.println("map span end: " +
						// entry.getKey().getSpanEnd() + " > annot span start: "
						// + span.getSpanStart() + " -- NOT exiting...");
						exitNow = false;
					}
				}
				if (exitNow) {
					break;
				}
			}
		}

		/*
		 * if no covering spans are detected then we add a null placeholder so that the
		 * for loops in toString will execute once
		 */
		if (identifiers.isEmpty()) {
			identifiers.add(null);
		}
		return identifiers;
	}

	/**
	 * @param type
	 * @param isActualType - true if not a concept annotation. If a concept
	 *                     annotation then true if this is the most specific type
	 *                     for this annotation, false if it is a parent of the
	 *                     actual concept type
	 * @param coveredText
	 * @param sectionId
	 * @param paragraphId
	 * @param sentenceId
	 * @param conceptId
	 * @param spanStart
	 * @param spanEnd
	 * @param annotationId
	 * @return
	 * @throws IOException
	 */
	private String serializeAnnotation(String annotationId, String annotatorName, Layer layer, String type,
			String coveredText, int spanStart, int spanEnd) {
		if (spanStart != spanEnd) {
			coveredText = coveredText.replaceAll("\\n", " ");
			coveredText = coveredText.replaceAll("\\t", " ");
			
			// if there is a double quote in the covered text, then we escape it with another double quote
			coveredText = coveredText.replaceAll("\"", "\"\"");

			String docId = documentId;
			if (docId.endsWith(".xml.gz.txt.gz")) {
				docId = StringUtil.removeSuffix(docId, ".xml.gz.txt.gz");
			}

//			String dateStamp = String.format("%s-%s-%s", publicationDate.get(Calendar.YEAR),
//					publicationDate.get(Calendar.MONTH) + 1, publicationDate.get(Calendar.DAY_OF_MONTH));

			// TODO: proper date handling at a later time
//			String dateStamp = (publicationYear == null) ? "2019-1-1"
//					: String.format("%d-%d-%d", publicationYear, 1, 1);

			return annotationId + "\t" + annotatorName + "\t" + docId + "\t" + layer.name() + "\t" + type + "\t"
					+ spanStart + "\t" + spanEnd + "\t\"" + coveredText + "\"";
		}
		return null;
	}

	@Data
	public static class Relation {
		private final String source;
		private final String type;
		private final String relatedAnnotationId;
	}

}
