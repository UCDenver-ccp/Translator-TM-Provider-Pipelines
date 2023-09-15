package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentTextAugmentationFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * 
 * This class was originally designed to add extra sentences to the bottom of
 * the document so that they will be processed by the concept recognition
 * components when the full document is processed. The original use was to
 * process sentences that contain an abbreviation definition with the short form
 * of the definition removed. For example, there may be a sentence that contains
 * "embryonic stem (ES) cells". OGER does not match that text to the CL concept
 * for embryonic stem cells, however if we remove the short form from the
 * abbreviation definition, OGER will match the text "embryonic stem cells". The
 * end of the document is marked with an indicator that will be used to remove
 * the added text if needed.
 *
 */
public class DocumentTextAugmentationPipeline {

//	private final static Logger LOGGER = Logger.getLogger(BigQueryExportPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.DOC_TEXT_AUGMENTATION;

	public interface Options extends DataflowPipelineOptions {
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getTextPipelineKey();

		void setTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getTextPipelineVersion();

		void setTextPipelineVersion(String value);

		@Description("This pipeline key will be used to select the input sentence documents that will be processed")
		@Required
		PipelineKey getSentencePipelineKey();

		void setSentencePipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input sentence documents that will be processed")
		@Required
		String getSentencePipelineVersion();

		void setSentencePipelineVersion(String value);

		@Description("This pipeline version will be used to select the input abbreviation documents that will be processed")
		@Required
		String getAbbreviationPipelineVersion();

		void setAbbreviationPipelineVersion(String value);

		@Description("This pipeline version will be used as part of the output document key/name")
		@Required
		String getOutputPipelineVersion();

		void setOutputPipelineVersion(String value);

		@Description("The name of the document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous imported documents")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);
	}

	public static void main(String[] args) {
//		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.TEXT_AUG_DONE;
		Pipeline p = Pipeline.create(options);

		// require that documents have the original text, abbreviations, and segmented
		// sentences
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.ABBREVIATIONS_DONE, ProcessingStatusFlag.SENTENCE_DONE);

		/*
		 * The OGER pipeline requires plain text documents, hence the type=TEXT and
		 * format=TEXT below. However, the selection of documents can be adjusted by
		 * specifying different pipeline keys and pipeline versions. These are set in
		 * the options for this pipeline.
		 */
		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextPipelineKey(), options.getTextPipelineVersion());
		DocumentCriteria inputSentDocCriteria = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				options.getSentencePipelineKey(), options.getSentencePipelineVersion());
		DocumentCriteria inputAbbrevDocCriteria = new DocumentCriteria(DocumentType.ABBREVIATIONS,
				DocumentFormat.BIONLP, PipelineKey.ABBREVIATION, options.getAbbreviationPipelineVersion());

		Set<DocumentCriteria> requiredDocCriteria = CollectionsUtil.createSet(inputTextDocCriteria,
				inputSentDocCriteria, inputAbbrevDocCriteria);
		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(requiredDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria augDocTextOutputDocCriteria = new DocumentCriteria(DocumentType.AUGMENTED_TEXT,
				DocumentFormat.TEXT, PIPELINE_KEY, options.getOutputPipelineVersion());
		DocumentCriteria augSentBionlpOutputDocCriteria = new DocumentCriteria(DocumentType.AUGMENTED_SENTENCE,
				DocumentFormat.BIONLP, PIPELINE_KEY, options.getOutputPipelineVersion());

		PCollectionTuple output = DocumentTextAugmentationFn.process(statusEntity2Content, augDocTextOutputDocCriteria,
				requiredDocCriteria, timestamp, PIPELINE_KEY);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAugmentedText = output
				.get(DocumentTextAugmentationFn.AUGMENTED_TEXT_TAG);
		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAugmentedSentenceBionlp = output
				.get(DocumentTextAugmentationFn.AUGMENTED_SENTENCE_TAG);
		PCollection<EtlFailureData> failures = output.get(DocumentTextAugmentationFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToAugmentedText.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the augmented document text (this does not include the original
		 * document text in order to avoid duplicating storage)
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToAugmentedText);
		nonredundantDocIdToAnnotations
				.apply("annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(augDocTextOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the augmented sentences in bionlp format -- these appended to the
		 * original sentences prior to downstream processing
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToAugSentAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToAugmentedSentenceBionlp);
		nonredundantDocIdToAugSentAnnotations
				.apply("aug sent annot->annot_entity",
						ParDo.of(new DocumentToEntityFn(augSentBionlpOutputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("aug sent annot_entity->datastore",
						DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();

	}

}
