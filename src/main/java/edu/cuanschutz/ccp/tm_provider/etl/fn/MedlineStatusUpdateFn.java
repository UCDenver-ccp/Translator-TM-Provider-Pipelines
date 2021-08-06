package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.medline.PubmedArticle;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;

/**
 * Updates status entities for Medline documents by adding the publication date
 * and publication types
 *
 */
public class MedlineStatusUpdateFn extends DoFn<PubmedArticle, KV<String, List<String>>> {

	private static final long serialVersionUID = 1L;

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
	public static PCollectionTuple process(PCollection<KV<String, CoGbkResult>> pubmedArticleAndStatusMergeResult,
			TupleTag<PubmedArticle> articleTag, TupleTag<ProcessingStatus> statusEntityTag,
			com.google.cloud.Timestamp timestamp) {

		return pubmedArticleAndStatusMergeResult.apply("update status entities",
				ParDo.of(new DoFn<KV<String, CoGbkResult>, ProcessingStatus>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<String, CoGbkResult> element = context.element();
						CoGbkResult result = element.getValue();
						String pmid = element.getKey();

						/*
						 * the result should have one line from each file except for the header in the
						 * bert output file which won't have a corresponding match in the metadata file.
						 * Below we get one line for each tag, check to make sure there aren't other
						 * lines just in case, and handle the case of the bert output file header.
						 */
						PubmedArticle pubmedArticle = null;
						Iterator<PubmedArticle> pubmedArticleIter = result.getAll(articleTag).iterator();
						if (pubmedArticleIter.hasNext()) {
							pubmedArticle = pubmedArticleIter.next();
							// ensure that the pubmedArticle has the correct id - note that there are
							// sometime >1 pubmed articles stored in the iterator - but they all seem to
							// have the correct id.
							if (!("PMID:" + pubmedArticle.getMedlineCitation().getPMID().getvalue()).equals(pmid)) {
								throw new IllegalArgumentException(
										"Extracted PubMed article has unexpected PMID. Expected: " + element.getKey()
												+ " but observed: "
												+ pubmedArticle.getMedlineCitation().getPMID().getvalue());
							}
						}

						ProcessingStatus statusEntity = null;
						Iterator<ProcessingStatus> statusEntityIter = result.getAll(statusEntityTag).iterator();
						if (statusEntityIter.hasNext()) {
							statusEntity = statusEntityIter.next();
							if (!statusEntity.getDocumentId().equals(pmid)) {
								throw new IllegalArgumentException(
										"Extracted status entity has unexpected PMID. Expected: " + element.getKey()
												+ " bu observed: " + statusEntityIter.next().getDocumentId());
							}
						}

						/*
						 * if there is a match - then update the status entity with data from the pubmed
						 * article
						 */
						if (statusEntity != null && pubmedArticle != null) {
							ProcessingStatus updatedStatusEntity = new ProcessingStatus(statusEntity);
							try {
								String yearPublished = MedlineXmlToTextFn
										.getYearPublished(pubmedArticle.getMedlineCitation());
								updatedStatusEntity.setYearPublished(yearPublished);
							} catch (NullPointerException e) {
								updatedStatusEntity.setYearPublished(MedlineXmlToTextFn.DEFAULT_PUB_YEAR);
								EtlFailureData failure = new EtlFailureData(
										new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
												PipelineKey.UPDATE_MEDLINE_STATUS_ENTITIES, "0.1"),
										"Failure while extracting publication year from Medline XML",
										statusEntity.getDocumentId(), timestamp);
								context.output(etlFailureTag, failure);
							}

							try {
								List<String> publicationTypes = MedlineXmlToTextFn
										.getPublicationTypes(pubmedArticle.getMedlineCitation());
								for (String pubType : publicationTypes) {
									updatedStatusEntity.addPublicationType(pubType);
								}
							} catch (NullPointerException e) {
								updatedStatusEntity.addPublicationType(MedlineXmlToTextFn.UNKNOWN_PUBLICATION_TYPE);
								EtlFailureData failure = new EtlFailureData(
										new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
												PipelineKey.UPDATE_MEDLINE_STATUS_ENTITIES, "0.1"),
										"Failure while extracting publication types from Medline XML",
										statusEntity.getDocumentId(), timestamp);
								context.output(etlFailureTag, failure);
							}

							context.output(processingStatusTag, updatedStatusEntity);

						}
					}
				}).withOutputTags(processingStatusTag, TupleTagList.of(etlFailureTag)));
	}

}
