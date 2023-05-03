package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.medline.Abstract;
import org.medline.AbstractText;
import org.medline.MedlineCitation;
import org.medline.PublicationType;
import org.medline.PubmedArticle;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * Outputs four {@link PCollection} objects
 * <ul>
 * <li>mapping document ID to plain text</li>
 * <li>mapping document ID to a serialized (BioNLP) form of the section
 * annotations -- in the case of Medline documents the sections are Title and
 * Abstract</li>
 * <li>a log of any failures</li>
 * <li>a status object that indicates which jobs still need processing, e.g.
 * dependency parse, etc.</li>
 * </ul>
 *
 */
public class MedlineXmlToTextFn extends DoFn<PubmedArticle, KV<String, List<String>>> {

	// 2155 is the max year value type in MySQL
	public static final String DEFAULT_PUB_YEAR = "2155";

	private static final long serialVersionUID = 1L;

	public static final String UNKNOWN_PUBLICATION_TYPE = "Unknown";

	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> plainTextTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> sectionAnnotationsTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
	};

	/**
	 * @param pubmedArticles
	 * @param outputTextDocCriteria
	 * @param outputAnnotationDocCriteria
	 * @param timestamp
	 * @param collection
	 * @param docIdsAlreadyInDatastore
	 * @return
	 */
	public static PCollectionTuple process(PCollection<PubmedArticle> pubmedArticles,
			DocumentCriteria outputTextDocCriteria, com.google.cloud.Timestamp timestamp, String collection,
			PCollectionView<Set<String>> docIdsAlreadyInDatastore, OverwriteOutput overwrite) {

		return pubmedArticles.apply("Extract section annotations",
				ParDo.of(new DoFn<PubmedArticle, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						PubmedArticle pubmedArticle = context.element();
						/* pubmedArticle was observed to be null in the daily update files */
						if (pubmedArticle != null) {
							Set<String> alreadyStoredDocIds = context.sideInput(docIdsAlreadyInDatastore);
							TextDocumentWithMetadata td = buildDocument(pubmedArticle);
							if (td != null) {
								// only store new documents unless overwrite = OverwriteOutput.YES
								if (overwrite == OverwriteOutput.YES
										|| !alreadyStoredDocIds.contains(td.getSourceid())) {
									try {
										outputDocument(context, td, collection);
									} catch (Throwable t) {
										EtlFailureData failure = new EtlFailureData(outputTextDocCriteria,
												"Likely failure during Medline processing.", td.getSourceid(), t,
												timestamp);
										context.output(etlFailureTag, failure);
									}
								}
							}
						}
					}
				}).withSideInputs(docIdsAlreadyInDatastore).withOutputTags(plainTextTag,
						TupleTagList.of(sectionAnnotationsTag).and(etlFailureTag).and(processingStatusTag)));
	}

	/**
	 * @param pubmedArticle
	 * @return a {@link TextDocument} containing the title/abstract text and
	 *         corresponding section annotations
	 */
	static TextDocumentWithMetadata buildDocument(PubmedArticle pubmedArticle) {
		MedlineCitation medlineCitation = pubmedArticle.getMedlineCitation();
		/*
		 * There are cases when processing Medline update files where either the
		 * MedlineCitation or the PMID are null. This is likely when processing the
		 * DeleteCitation entries at the bottom of each update file. To handle these
		 * cases, we check for nulls here and skip any documents where the
		 * MedlineCitation or PMID are null
		 */
		if (medlineCitation != null && medlineCitation.getPMID() != null) {
			String pmid = "PMID:" + medlineCitation.getPMID().getvalue();

			/*
			 * the titleAnnotations and abstractAnnotations lists will store annotations for
			 * sub- and superscript text observed in the title and abstract, respectively
			 */
			List<TextAnnotation> titleAnnotations = new ArrayList<TextAnnotation>();
			List<TextAnnotation> abstractAnnotations = new ArrayList<TextAnnotation>();

			String title = extractTitleText(medlineCitation, titleAnnotations);
			String abstractText = getAbstractText(pubmedArticle, abstractAnnotations);
			String documentText = (abstractText == null || abstractText.isEmpty()) ? title
					: String.format("%s\n\n%s", title, abstractText);

			String yearPublished = getYearPublished(medlineCitation);
			List<String> publicationTypes = getPublicationTypes(medlineCitation);

			TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(pmid);
			TextAnnotation titleAnnotation = factory.createAnnotation(0, title.length(), title, "title");
			TextAnnotation abstractAnnotation = null;
			if (abstractText != null && !abstractText.isEmpty()) {
				int abstractStart = title.length() + 2;
				int abstractEnd = abstractStart + abstractText.length();
				abstractAnnotation = factory.createAnnotation(abstractStart, abstractEnd, abstractText, "abstract");

				/*
				 * if there are any abstractAnnotations -- these are sub- and super-script
				 * annotations found in the abstract then we need to adjust their spans based on
				 * the start offset of the abstract
				 */

				if (!abstractAnnotations.isEmpty()) {
					for (TextAnnotation annot : abstractAnnotations) {
						List<Span> updatedSpans = new ArrayList<Span>();
						for (Span span : annot.getSpans()) {
							updatedSpans.add(
									new Span(span.getSpanStart() + abstractStart, span.getSpanEnd() + abstractStart));
						}
						annot.setSpans(updatedSpans);
					}
				}

			}

			TextDocumentWithMetadata td = new TextDocumentWithMetadata(pmid, "PubMed", documentText);
			td.addAnnotation(titleAnnotation);
			if (abstractAnnotation != null) {
				td.addAnnotation(abstractAnnotation);
			}
			if (!titleAnnotations.isEmpty()) {
				td.addAnnotations(titleAnnotations);
			}
			if (!abstractAnnotations.isEmpty()) {
				td.addAnnotations(abstractAnnotations);
			}
			td.setYearPublished(yearPublished);
			for (String pubType : publicationTypes) {
				td.addPublicationType(pubType);
			}
			return td;
		}
		return null;
	}

	public static String extractTitleText(MedlineCitation medlineCitation, List<TextAnnotation> titleAnnotations) {
		return processTitleAndAbstractText(medlineCitation.getArticle().getArticleTitle().getvalue(), titleAnnotations);
	}

	public static String getYearPublished(MedlineCitation medlineCitation) {
		List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate = medlineCitation.getArticle().getJournal().getJournalIssue()
				.getPubDate().getYearOrMonthOrDayOrSeasonOrMedlineDate();
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof org.medline.Year) {
				org.medline.Year year = (org.medline.Year) obj;
				return year.getvalue();
			}
			if (obj instanceof org.medline.MedlineDate) {
				org.medline.MedlineDate md = (org.medline.MedlineDate) obj;
				return extractYearFromMedlineDate(md);
			}

		}
		return DEFAULT_PUB_YEAR;
	}

	protected static String extractYearFromMedlineDate(org.medline.MedlineDate md) {
		// <MedlineDate>1998 Dec-1999 Jan</MedlineDate>
		// <MedlineDate>2015 Nov-Dec</MedlineDate>
		/* year should be 1st four characters */
		String yearStr = md.getvalue().split(" ")[0];
		if (yearStr.matches("[1-2][0-9][0-9][0-9]")) {
			return yearStr;
		}
		return DEFAULT_PUB_YEAR;
	}

	public static List<String> getPublicationTypes(MedlineCitation medlineCitation) {
		List<String> pTypes = new ArrayList<String>();
		for (PublicationType pt : medlineCitation.getArticle().getPublicationTypeList().getPublicationType()) {
			pTypes.add(pt.getvalue());
		}
		return pTypes;
	}

	/**
	 * @param pubmedArticle
	 * @param annotations
	 * @return the abstract text compiled from the {@link PubmedArticle}
	 */
	public static String getAbstractText(PubmedArticle pubmedArticle, List<TextAnnotation> annotations) {
		Abstract theAbstract = pubmedArticle.getMedlineCitation().getArticle().getAbstract();
		if (theAbstract == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (AbstractText aText : theAbstract.getAbstractText()) {
			String text = aText.getvalue();

			if (sb.length() == 0) {
				sb.append(text);
			} else {
				sb.append("\n" + text);
			}
		}

		return processTitleAndAbstractText(sb.toString(), annotations);
	}

	/**
	 * Replace multiple whitespace with single space. Remove <b>, <i>, <u>, <sup>,
	 * <sub>
	 * 
	 * @param observedAnnotations annotations indicating where sub- and superscript
	 *                            text was observed in the input text (title or
	 *                            abstract)
	 * 
	 * @param aText
	 * @return
	 */
	private static String processTitleAndAbstractText(String text, List<TextAnnotation> observedAnnotations) {
		String updatedText = text.trim();
		// below is a special space being replaced by a regular space
		updatedText = updatedText.replaceAll(" ", " ");

		// there some records that have line breaks and lots of extra spaces -- these
		// need to be treated differently when the line break occurs just prior to or
		// just after a tag. See https://pubmed.ncbi.nlm.nih.gov/31000267/
		updatedText = updatedText.replaceAll("\\n\\s+<", "<");
		updatedText = updatedText.replaceAll(">\\n\\s+", ">");

		updatedText = updatedText.replaceAll("<b>", "");
		updatedText = updatedText.replaceAll("</b>", "");
		updatedText = updatedText.replaceAll("<i>", "");
		updatedText = updatedText.replaceAll("</i>", "");
		updatedText = updatedText.replaceAll("<u>", "");
		updatedText = updatedText.replaceAll("</u>", "");
		updatedText = updatedText.replaceAll("\\s\\s+", " ");

		updatedText = updatedText.replaceAll("&lt;", "<");
		updatedText = updatedText.replaceAll("&gt;", ">");
		updatedText = updatedText.replaceAll("&amp;", "&");
		updatedText = updatedText.replaceAll("&quot;", "\"");
		updatedText = updatedText.replaceAll("&apos;", "'");

		/*
		 * once we have replaced the above HTML tags and escaped XML characters, we will
		 * replace the subscript and superscript tags. While doing so, we will add
		 * "section" annotations for these tags so that downstream tools will be able to
		 * recover the subscript and superscript information.
		 */

		observedAnnotations.addAll(extractSubNSuperscriptAnnotations(updatedText));
		updatedText = updatedText.replaceAll("<sub>", "");
		updatedText = updatedText.replaceAll("</sub>", "");
		updatedText = updatedText.replaceAll("<sup>", "");
		updatedText = updatedText.replaceAll("</sup>", "");
		updatedText = updatedText.replaceAll("<sub/>", "");
		updatedText = updatedText.replaceAll("<sup/>", "");
		validateObservedAnnotations(observedAnnotations, updatedText);
		return updatedText;
	}

	/**
	 * Makes sure that the annotations created to store sub- and superscript
	 * information use spans that align with the document text.
	 * 
	 * @param observedAnnotations
	 * @param updatedText
	 */
	private static void validateObservedAnnotations(List<TextAnnotation> observedAnnotations, String updatedText) {
		for (TextAnnotation annot : observedAnnotations) {
			String expectedText = annot.getCoveredText();
			String observedText = updatedText.substring(annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd());
			if (!observedText.equals(expectedText)) {
				throw new IllegalStateException(String.format(
						"Error during Medline XML parsing - extracted annotation text (likely for sub or superscript "
								+ "annotation) does not match as expected. '%s' != '%s'",
						expectedText, observedText));
			}
		}

	}

	/**
	 * Creates annotations indicating where sub- and superscript text was observed
	 * in the specified text
	 * 
	 * @param text
	 * @return
	 */
	private static Collection<? extends TextAnnotation> extractSubNSuperscriptAnnotations(String text) {
		List<TextAnnotation> annots = new ArrayList<TextAnnotation>();
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		String updatedText = text;
		// handle <sub/> just in case?
		List<String> tags = Arrays.asList("sup", "sub");
		Stack<TextAnnotation> stack = new Stack<TextAnnotation>();
		while (containsTag(updatedText, tags)) {
			Tag tag = getNextTag(updatedText, tags);
			if (tag.getType() == Tag.Type.OPEN) {
				// add new annotation to the stack
				TextAnnotation annot = factory.createAnnotation(tag.getStart(), tag.getStart() + 1, "",
						tag.getTagText());
				stack.push(annot);
			} else if (tag.getType() != Tag.Type.EMPTY) {
				// pop the stack, complete the annotation and add it to the annots list unless
				// this is an empty tag, e.g. <sub/>
				TextAnnotation annot = stack.pop();

				// validate that the tag type of the popped annot matches the expected tag type
				if (!tag.getTagText().equals(annot.getClassMention().getMentionName())) {
					throw new IllegalStateException(String.format("Popped annot not of expected type: %s != %s",
							tag.getTagText(), annot.getClassMention().getMentionName()));
				}

				int end = tag.getStart();
				int start = annot.getAnnotationSpanStart();
				annot.setSpan(new Span(start, end));
				annot.setCoveredText(updatedText.substring(start, end));
				annots.add(annot);
			}
			// update the text by removing the tag that was just processed
			updatedText = updatedText.replaceFirst(tag.getRegex(), "");
		}

		/* the stack should be empty at this point */
		if (!stack.isEmpty()) {
			throw new IllegalStateException("Annotation stack should be empty at this point.");
		}

		return annots;
	}

	private static Tag getNextTag(String text, List<String> tags) {
		Map<Integer, Tag> startIndexToTagMap = new HashMap<Integer, Tag>();
		for (String tagStr : tags) {
			for (Tag.Type type : Tag.Type.values()) {
				Tag tag = new Tag(tagStr, type);
				int index = text.indexOf(tag.getHtmlTag());
				if (index >= 0) {
					// unit tests suggested -1 needed here.
					tag.setStart(index);
					startIndexToTagMap.put(index, tag);
				}
			}
		}

		/* there needs to be at least one tag in the map */
		if (startIndexToTagMap.size() < 1) {
			throw new IllegalStateException("Expected to find tag, but did not.");
		}

		/* sort the map by tag start index, then choose the first tag and return */
		Map<Integer, Tag> sortedMap = CollectionsUtil.sortMapByKeys(startIndexToTagMap, SortOrder.ASCENDING);

		/* return the first tag */
		return sortedMap.entrySet().iterator().next().getValue();

	}

	private static boolean containsTag(String text, List<String> tags) {
		for (String tagStr : tags) {
			for (Tag.Type type : Tag.Type.values()) {
				Tag tag = new Tag(tagStr, type);
				if (text.contains(tag.getHtmlTag())) {
					return true;
				}
			}
		}
		return false;
	}

	@Data
	private static class Tag {
		public enum Type {
			OPEN, CLOSE, EMPTY;
		}

		private int start;
		private final String tagText;
		private final Type type;

		private String getHtmlTag() {
			return String.format("<%s%s%s>", (type == Type.CLOSE ? "/" : ""), tagText, (type == Type.EMPTY ? "/" : ""));
		}

		private String getRegex() {
			return getHtmlTag();
		}
	}

	private static void outputDocument(ProcessContext context, TextDocumentWithMetadata td, String collection)
			throws IOException {
		String docId = td.getSourceid();
		String plainText = td.getText();

		/* serialize the annotations into the BioNLP format */
		BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		bionlpWriter.serialize(td, baos, CharacterEncoding.UTF_8);
		String serializedAnnotations = baos.toString(CharacterEncoding.UTF_8.getCharacterSetName());

		/*
		 * divide the document content into chunks if necessary so that each chunk is
		 * under the DataStore byte length threshold
		 */
		List<String> chunkedPlainText = PipelineMain.chunkContent(plainText);
		List<String> chunkedAnnotations = PipelineMain.chunkContent(serializedAnnotations);

		context.output(sectionAnnotationsTag, KV.of(docId, chunkedAnnotations));
		context.output(plainTextTag, KV.of(docId, chunkedPlainText));
		/*
		 * output a {@link ProcessingStatus} for the document
		 */
		ProcessingStatus status = new ProcessingStatus(docId);
		if (td.getYearPublished() != null) {
			status.setYearPublished(td.getYearPublished());
		} else {
			status.setYearPublished(DEFAULT_PUB_YEAR);
		}
		if (td.getPublicationTypes() != null && !td.getPublicationTypes().isEmpty()) {
			for (String pt : td.getPublicationTypes()) {
				status.addPublicationType(pt);
			}
		} else {
			status.addPublicationType(UNKNOWN_PUBLICATION_TYPE);
		}
		status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
		status.enableFlag(ProcessingStatusFlag.SECTIONS_DONE);

		if (collection != null) {
			status.addCollection(collection);
		}
		context.output(processingStatusTag, status);
	}

	private static class TextDocumentWithMetadata extends TextDocument {

		@Setter
		@Getter
		private String yearPublished;

		@Getter
		private List<String> publicationTypes = new ArrayList<String>();

		public TextDocumentWithMetadata(String sourceid, String sourcedb, String text) {
			super(sourceid, sourcedb, text);
		}

		public void addPublicationType(String pubType) {
			publicationTypes.add(pubType);
		}

	}

}
