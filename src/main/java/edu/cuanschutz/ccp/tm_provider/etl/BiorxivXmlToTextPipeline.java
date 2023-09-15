package edu.cuanschutz.ccp.tm_provider.etl;

import static edu.cuanschutz.ccp.tm_provider.etl.fn.JatsArticleToDocumentFn.etlFailureTag;
import static edu.cuanschutz.ccp.tm_provider.etl.fn.JatsArticleToDocumentFn.processingStatusTag;
import static edu.cuanschutz.ccp.tm_provider.etl.fn.JatsArticleToDocumentFn.sectionAnnotationsTag;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.biorxiv.Article;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.JatsArticleToDocumentFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.JatsDocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.JatsFileToArticleFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

public class BiorxivXmlToTextPipeline {

	public final static Logger LOGGER = Logger.getLogger(BiorxivXmlToTextPipeline.class.getName());

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		@Required
		String getBiorxivXmlDir();

		void setBiorxivXmlDir(String value);

		@Description("The name of the document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous imported documents")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);
	}

	public static void main(String[] args) {
		// region Set up pipeline
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		MedlineXmlToTextPipeline.Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(MedlineXmlToTextPipeline.Options.class);
		Pipeline p = Pipeline.create(options);
		// endregion

		// region Read and process Biorxiv/Medrxiv files
		String biorxivXmlFilePattern = "biorxiv-data/*.xml";
		PCollection<Article> articles = p
				.apply("Get XML files to load", FileIO.match().filepattern(biorxivXmlFilePattern))
				.apply("Read in XML files", FileIO.readMatches())
				.apply("Parse XML files to Articles", ParDo.of(new JatsFileToArticleFn()));

		final PCollectionView<Set<String>> existingDocIds = PipelineMain.catalogExistingDocuments(options.getProject(),
				options.getCollection(), options.getOverwrite(), p);

		DocumentCriteria sectionCriteria = new DocumentCriteria(DocumentType.SECTIONS, DocumentFormat.BIONLP,
				PipelineKey.BIORXIV_XML_TO_TEXT, pipelineVersion);
		Map<String, DocumentCriteria> criteriaMap = Collections.singletonMap("sections", sectionCriteria);

		PCollectionTuple outputs = articles.apply(ParDo
				.of(new JatsArticleToDocumentFn(timestamp, criteriaMap, existingDocIds)).withSideInputs(existingDocIds)
				.withOutputTags(sectionAnnotationsTag, TupleTagList.of(etlFailureTag).and(processingStatusTag)));
		// endregion

		// region Retrieve the various outputs from processing the articles.
		PCollection<ProcessingStatus> statuses = outputs.get(JatsArticleToDocumentFn.processingStatusTag);
		PCollection<KV<String, List<String>>> sections = outputs.get(sectionAnnotationsTag);
		PCollection<EtlFailureData> failures = outputs.get(etlFailureTag);
		// endregion

		// region Store the Status data in Cloud DataStore
		// Convert Status to a status Entity map
		PCollection<KV<String, Entity>> statusEntities = statuses.apply("status->status_entity",
				ParDo.of(new ProcessingStatusToEntityFn()));

		// Uniquify the Status Entity map
		PCollection<Entity> uniqueStatusEntities = PipelineMain.deduplicateEntitiesByKey(statusEntities);

		// Save to DataStore
		uniqueStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));
		// endregion

		// region Store the Section Annotation data in the Cloud DataStore
		// Create a map between document IDs and document Collection(s)
		PCollectionView<Map<String, Set<String>>> collectionsMap = uniqueStatusEntities
				.apply(ParDo.of(new DoFn<Entity, KV<String, Set<String>>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						Entity statusEntity = c.element();
						ProcessingStatus ps = new ProcessingStatus(statusEntity);
						c.output(KV.of(ps.getDocumentId(), ps.getCollections()));
					}
				})).apply(View.asMap());

		PCollection<KV<String, List<String>>> nonredundantSectionAnnotations = PipelineMain
				.deduplicateDocumentsByStringKey(sections);
		// Convert Section Annotation to Entity, and Save to DataStore
		nonredundantSectionAnnotations.apply("section annotation -> annotation entity",
				ParDo.of(new JatsDocumentToEntityFn(sectionCriteria, collectionsMap)).withSideInputs(collectionsMap))
				.apply("section annotation entity -> datastore",
						DatastoreIO.v1().write().withProjectId(options.getProject()));
		// endregion

		// region Store the Failure data in Cloud DataStore
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failure->failure_entity",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));
		// endregion

		p.run().waitUntilFinish();
	}
}
