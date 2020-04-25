package edu.cuanschutz.ccp.tm_provider.etl.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.serialization.BigQueryAnnotationSerializer.TableKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.conllu.CoNLLUDocumentReader;
import edu.ucdenver.ccp.file.conversion.pubannotation.PubAnnotationDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class PubAnnotationFormatter implements Serializable {

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
	public String toPubAnnotationJson(String docId, String sourceStrContainsYear,
			Map<DocumentType, String> docTypeToContent) throws IOException {

		TextDocument td = extractAllAnnotations(docId, sourceStrContainsYear, docTypeToContent);
		PubAnnotationDocumentWriter docWriter = new PubAnnotationDocumentWriter();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		docWriter.serialize(td, outputStream, CharacterEncoding.UTF_8);

		return new String(outputStream.toByteArray());

	}

	/**
	 * TODO: this code is duplicated in BigQueryExportFileBuilderFN -- move it to a
	 * single location Extract all annotations from the annotation files that have
	 * accompanied the document. Some assumptions are made regarding annotator names
	 * to assign.
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

}
