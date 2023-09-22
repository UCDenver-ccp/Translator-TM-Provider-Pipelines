package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
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

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.OgerPostProcessingFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * 
 * Compares OGER concept annotations to the dictionary entries and removes
 * spurious matches
 * 
 * 
 */
public class OgerPostProcessingPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.OGER_POST_PROCESS;

	public interface Options extends DataflowPipelineOptions {
		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		@Required
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("pipe-delimited list of processing status flags that will be used to query for status entities from Datastore")
		@Required
		String getRequiredProcessingStatusFlags();

		void setRequiredProcessingStatusFlags(String flags);

		@Description("path to the ID-to-Oger-Dict-Entry map file")
		@Required
		String getIdToOgerDictEntryMapFilePath();

		void setIdToOgerDictEntryMapFilePath(String path);

		@Description("delimiter used to separate columns in the ID-to-Oger-Dict-Entry map file")
		@Required
		Delimiter getIdToOgerDictEntryMapFileDelimiter();

		void setIdToOgerDictEntryMapFileDelimiter(Delimiter delimiter);

		@Description("OGER_PP1_DONE or OGER_PP2_DONE-- depending on which phase is being run, Part 1 or Part 2 of the OGER post processing, this parameter also controls how the output will be stored via setting the DocumentType to be CONCEPT_OGER_PP1 or CONCEPT_OGER_PP2 in the code.")
		@Required
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag status);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("The version that is assigned to the Datastore documents created by this pipeline")
		@Required
		String getOutputPipelineVersion();

		void setOutputPipelineVersion(String value);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();

		DocumentType outputDocumentType = null;
		switch (targetProcessingStatusFlag) {
		case OGER_PP1_DONE:
			outputDocumentType = DocumentType.CONCEPT_OGER_PP1;
			break;
		case OGER_PP2_DONE:
			outputDocumentType = DocumentType.CONCEPT_OGER_PP2;
			break;
		default:
			throw new IllegalArgumentException(String.format(
					"Unexpected target processing status flag. Expected OGER_PP1_DONE or OGER_PP2_DONE but observed %s.",
					targetProcessingStatusFlag.name()));
		}

		Pipeline p = Pipeline.create(options);

		Set<ProcessingStatusFlag> requiredProcessStatusFlags = PipelineMain
				.compileRequiredProcessingStatusFlags(options.getRequiredProcessingStatusFlags());

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		final PCollectionView<Map<String, String>> idToOgerDictEntriesMapView = PCollectionUtil
				.fromTwoColumnFiles("id to oger dict entry map", p, options.getIdToOgerDictEntryMapFilePath(),
						options.getIdToOgerDictEntryMapFileDelimiter(), Compression.GZIP)
				.apply(View.<String, String>asMap());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(outputDocumentType, DocumentFormat.BIONLP,
				PIPELINE_KEY, options.getOutputPipelineVersion());

		PCollectionTuple output = OgerPostProcessingFn.process(statusEntity2Content, outputDocCriteria, timestamp,
				inputDocCriteria, idToOgerDictEntriesMapView, PIPELINE_KEY);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAnnotation = output
				.get(OgerPostProcessingFn.ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(OgerPostProcessingFn.ETL_FAILURE_TAG);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusEntityToAnnotation.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());
		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantDocIdToAnnotations = PipelineMain
				.deduplicateDocuments(statusEntityToAnnotation);
		nonredundantDocIdToAnnotations
				.apply("annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(outputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

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
