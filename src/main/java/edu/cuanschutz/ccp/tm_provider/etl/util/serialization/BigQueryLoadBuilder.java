package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Layer;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.Relation;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.TableKey;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.conllu.CoNLLUDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.SpanUtils;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.mention.ClassMention;
import edu.ucdenver.ccp.nlp.core.mention.ComplexSlotMention;

public class BigQueryLoadBuilder implements Serializable {

	private static final long serialVersionUID = 1L;

	private Map<TableKey, StringBuilder> keyToStringBuilderMap;

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
	public Map<TableKey, String> toBigQueryString(String docId, String sourceStrContainsYear,
			Map<DocumentType, String> docTypeToContent) throws IOException {

		keyToStringBuilderMap = new HashMap<TableKey, StringBuilder>();

		TextDocument td = extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);

		BigQueryAnnotationSerializer serializer = initBigQueryAnnotationSerializer(td);

		List<TextAnnotation> secondPassAnnotations = new ArrayList<TextAnnotation>();
		for (TextAnnotation annot : td.getAnnotations()) {
			if (isFirstPassAnnotation(annot)) {
				processAnnotation(td.getText(), annot, serializer);
			} else {
				secondPassAnnotations.add(annot);
			}
		}

		// now do 2nd pass annotations if there are any
		for (TextAnnotation annot : secondPassAnnotations) {
			processAnnotation(td.getText(), annot, serializer);
		}

		Map<TableKey, String> returnMap = new HashMap<TableKey, String>();
		for (Entry<TableKey, StringBuilder> entry : keyToStringBuilderMap.entrySet()) {
			String value = entry.getValue().toString();
			while (StringUtil.endsWithRegex(value, "\\s")) {
				value = value.substring(0, value.length() - 1);
			}
			returnMap.put(entry.getKey(), value);
		}
//		keyToStringBuilderMap.entrySet().forEach(e -> returnMap.put(e.getKey(), e.getValue().toString()));
		return returnMap;
	}

	/**
	 * Serialize the specified TextAnnotation, adding lines to the appropriate
	 * 'table' String Builders
	 * 
	 * @param td
	 * @param annot
	 * @param serializer
	 */
	private void processAnnotation(String documentText, TextAnnotation annot, BigQueryAnnotationSerializer serializer) {
		Map<TableKey, Set<String>> storageStrings = serializer.toString(annot, documentText);
		for (Entry<TableKey, Set<String>> entry : storageStrings.entrySet()) {
			StringBuilder builder = getStringBuilder(entry.getKey());
			entry.getValue().forEach(s -> builder.append(s + "\n"));
		}
	}

//	private Map<String, Set<String>> populateSectionIdToNormalizedTypeMap() {
//		// TODO Auto-generated method stub
//		return null;
//	}

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
	static TextDocument extractAllAnnotations(String docId, String sourceStrContainsYear,
			Map<DocumentType, String> docTypeToContent) throws IOException {
		String text = docTypeToContent.get(DocumentType.TEXT);
		TextDocument td = new TextDocument(docId, sourceStrContainsYear, text);

		for (Entry<DocumentType, String> entry : docTypeToContent.entrySet()) {
			if (entry.getKey() == DocumentType.DEPENDENCY_PARSE) {
				CoNLLUDocumentReader reader = new CoNLLUDocumentReader();
				TextDocument doc = reader.readDocument(docId, sourceStrContainsYear,
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);
				for (TextAnnotation ta : doc.getAnnotations()) {
					ta.setAnnotator(new Annotator(null, "turku", null));
				}

				td.addAnnotations(doc.getAnnotations());

			} else if (entry.getKey() == DocumentType.SECTIONS) {
				BioNLPDocumentReader reader = new BioNLPDocumentReader();
				TextDocument doc = reader.readDocument(docId, sourceStrContainsYear,
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);

				for (TextAnnotation ta : doc.getAnnotations()) {
					ta.setAnnotator(new Annotator(null, "bioc", null));
				}

				td.addAnnotations(doc.getAnnotations());
			} else if (entry.getKey().name().startsWith("CONCEPT_")) {
				BioNLPDocumentReader reader = new BioNLPDocumentReader();
				TextDocument doc = reader.readDocument(docId, sourceStrContainsYear,
						new ByteArrayInputStream(entry.getValue().trim().getBytes()),
						new ByteArrayInputStream(td.getText().trim().getBytes()), CharacterEncoding.UTF_8);

				// TODO: revisit this -- filter out EXT annotations
				List<TextAnnotation> keep = new ArrayList<TextAnnotation>();
				for (TextAnnotation ta : doc.getAnnotations()) {
					ta.setAnnotator(new Annotator(null, "oger", null));
					// exclude if contains EXT and if the part after the colon is not numbers,
					// otherwise, remove the _EXT and keep. Many of the PR and SO concepts follow
					// this pattern
					if (!ta.getClassMention().getMentionName().contains("EXT")) {
						keep.add(ta);
					} else {
						String[] toks = ta.getClassMention().getMentionName().split(":");
						if (toks[1].matches("\\d+")) {
							String updatedMentionName = ta.getClassMention().getMentionName().replace("_EXT", "");
							ta.getClassMention().setMentionName(updatedMentionName);
							keep.add(ta);
						}
					}
				}

				td.addAnnotations(keep);
			}
		}

		return td;
	}

	/**
	 * @param annot
	 * @return true if this annotation belongs in the 'first pass' of processing
	 */
	private boolean isFirstPassAnnotation(TextAnnotation annot) {
		Layer layer = BigQueryAnnotationSerializer.determineLayer(annot);
		return layer.isFirstPass();
	}

	/**
	 * @param td
	 * @param sectionIdToSectionTypeMap
	 * @return init the serializer
	 */
	private BigQueryAnnotationSerializer initBigQueryAnnotationSerializer(TextDocument td) {
		Map<Span, Set<String>> sectionSpanToId = new HashMap<Span, Set<String>>();
		Map<Span, Set<String>> paragraphSpanToId = new HashMap<Span, Set<String>>();
		Map<Span, Set<String>> sentenceSpanToId = new HashMap<Span, Set<String>>();
		Map<Span, Set<String>> conceptSpanToId = new HashMap<Span, Set<String>>();
		Map<String, Set<Relation>> annotationIdToRelationMap = new HashMap<String, Set<Relation>>();

		extractDocumentZoneAnnotations(td, sectionSpanToId, paragraphSpanToId, sentenceSpanToId, conceptSpanToId,
				annotationIdToRelationMap);

		String documentId = td.getSourceid();
//		Integer yearPublished = parseYearFromSourceStr(td.getSourcedb());

		return new BigQueryAnnotationSerializer(documentId, sectionSpanToId, paragraphSpanToId, sentenceSpanToId,
				conceptSpanToId, annotationIdToRelationMap);
	}

	/**
	 * for convenience, we will store the publication year as part of the source
	 * string in the text document
	 * 
	 * @param sourcedb
	 * @return
	 */
	private Integer parseYearFromSourceStr(String sourcedb) {
		Pattern p = Pattern.compile("year=(\\d\\d\\d\\d)");
		Matcher m = p.matcher(sourcedb);
		if (m.find()) {
			return Integer.parseInt(m.group(1));
		}
		return null;
	}

	/**
	 * @param td
	 * @param sectionSpanToId
	 * @param paragraphSpanToId
	 * @param sentenceSpanToId
	 * @param conceptSpanToId
	 * @param annotationIdToRelationMap
	 */
	static void extractDocumentZoneAnnotations(TextDocument td, Map<Span, Set<String>> sectionSpanToId,
			Map<Span, Set<String>> paragraphSpanToId, Map<Span, Set<String>> sentenceSpanToId,
			Map<Span, Set<String>> conceptSpanToId, Map<String, Set<Relation>> annotationIdToRelationMap) {

		for (TextAnnotation annot : td.getAnnotations()) {
			Layer layer = BigQueryAnnotationSerializer.determineLayer(annot);
			Span span = annot.getAggregateSpan();
//			String id = BigQueryUtil.getAnnotationIdentifier(annot, layer);
			String id = BigQueryUtil.getAnnotationIdentifier(td.getSourceid(), annot.getAggregateSpan().getSpanStart(),
					SpanUtils.getCoveredText(annot.getSpans(), td.getText()), layer,
					Arrays.asList(annot.getClassMention().getMentionName()));

			switch (layer) {
			case SECTION:
				CollectionsUtil.addToOne2ManyUniqueMap(span, id, sectionSpanToId);
				break;
			case PARAGRAPH:
				CollectionsUtil.addToOne2ManyUniqueMap(span, id, paragraphSpanToId);
				break;
			case SENTENCE:
				CollectionsUtil.addToOne2ManyUniqueMap(span, id, sentenceSpanToId);
				break;
			case CONCEPT:
				CollectionsUtil.addToOne2ManyUniqueMap(span, id, conceptSpanToId);
				break;
			case TOKEN:
				// catalog dependency relations
				Collection<ComplexSlotMention> csms = annot.getClassMention().getComplexSlotMentions();
				if (csms != null && csms.size() > 0) {
					// should only be one complex slot per token I think
					for (ComplexSlotMention csm : csms) {
						String dependencyRelation = csm.getMentionName();
						// should only be one classmention slot filler I think
						for (ClassMention cm : csm.getClassMentions()) {
							TextAnnotation relatedAnnot = cm.getTextAnnotation();
							Relation relation = new Relation("turku", dependencyRelation,
									BigQueryUtil.getAnnotationIdentifier(relatedAnnot, layer));
							CollectionsUtil.addToOne2ManyUniqueMap(id, relation, annotationIdToRelationMap);
						}
					}
				}

				break;

			default:
				// skip tokens, document
				break;
			}
		}
	}

	private StringBuilder getStringBuilder(TableKey key) {
		if (keyToStringBuilderMap.containsKey(key)) {
			return keyToStringBuilderMap.get(key);
		}

		StringBuilder builder = new StringBuilder();
		keyToStringBuilderMap.put(key, builder);
		return builder;
	}

}
