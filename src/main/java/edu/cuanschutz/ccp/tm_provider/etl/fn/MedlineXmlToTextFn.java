package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
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

	private static final String DEFAULT_PUB_YEAR = "9999";

	private static final long serialVersionUID = 1L;

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

		return pubmedArticles.apply("Extract title/abstract -- preserve section annotations",
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
			String title = medlineCitation.getArticle().getArticleTitle().getvalue();
			String abstractText = getAbstractText(pubmedArticle);
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
			}

			TextDocumentWithMetadata td = new TextDocumentWithMetadata(pmid, "PubMed", documentText);
			td.addAnnotation(titleAnnotation);
			if (abstractAnnotation != null) {
				td.addAnnotation(abstractAnnotation);
			}
			td.setYearPublished(yearPublished);
			for (String pubType : publicationTypes) {
				td.addPublicationType(pubType);
			}
			return td;
		}
		return null;
	}

	private static String getYearPublished(MedlineCitation medlineCitation) {
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

	private static List<String> getPublicationTypes(MedlineCitation medlineCitation) {
		List<String> pTypes = new ArrayList<String>();
		for (PublicationType pt : medlineCitation.getArticle().getPublicationTypeList().getPublicationType()) {
			pTypes.add(pt.getvalue());
		}
		return pTypes;
	}

	/**
	 * @param pubmedArticle
	 * @return the abstract text compiled from the {@link PubmedArticle}
	 */
	static String getAbstractText(PubmedArticle pubmedArticle) {
		Abstract theAbstract = pubmedArticle.getMedlineCitation().getArticle().getAbstract();
		if (theAbstract == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (AbstractText text : theAbstract.getAbstractText()) {
			if (sb.length() == 0) {
				sb.append(text.getvalue());
			} else {
				sb.append("\n" + text.getvalue());
			}
		}
		return sb.toString();
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
		}
		if (td.getPublicationTypes() != null) {
			for (String pt : td.getPublicationTypes()) {
				status.addPublicationType(pt);
			}
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
