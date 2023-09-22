package edu.cuanschutz.ccp.tm_provider.etl;

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

import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.FilterFlag;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptPostProcessingFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * This pipeline accomplishes a number of tasks:
 * <ul>
 * <li>Optionally filters concept annotations based on the CRF output
 * <li>converts any extension class concept identifiers to their corresponding
 * canonical OBO identifier
 * <li>performs post-processing operations on select ontologies (PR, NCBITaxon
 * currently)
 * <li>outputs mapping files from concept ids to ancestors and from concept ids
 * to concept labels
 * </ul>
 * 
 * will need to bring in OBO files, CRAFT extension class mapping files
 * 
 * 
 */
public class ConceptPostProcessingPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.CONCEPT_POST_PROCESS;

	public interface Options extends DataflowPipelineOptions {
		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		@Required
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		// TODO: InputDocumentCriteria and REquiredProcessingStatusFlags are redundant
		// in a way -- see if they can be combined

		@Description("pipe-delimited list of processing status flags that will be used to query for status entities from Datastore")
		@Required
		String getRequiredProcessingStatusFlags();

		void setRequiredProcessingStatusFlags(String flags);

//		@Description("path to the ID-to-Oger-Dict-Entry map file")
//		String getIdToOgerDictEntryMapFilePath();
//
//		void setIdToOgerDictEntryMapFilePath(String path);
//
//		@Description("delimiter used to separate columns in the ID-to-Oger-Dict-Entry map file")
//		Delimiter getIdToOgerDictEntryMapFileDelimiter();
//
//		void setIdToOgerDictEntryMapFileDelimiter(Delimiter delimiter);

//		@Description("delimiter used to separate values in the 2nd column in the ID-to-Oger-Dict-Entry map file")
//		Delimiter getIdToOgerDictEntryMapFileSetDelimiter();
//
//		void setIdToOgerDictEntryMapFileSetDelimiter(Delimiter delimiter);

		@Description("path to the NCBITaxon promotion map file")
		@Required
		String getNcbiTaxonPromotionMapFilePath();

		void setNcbiTaxonPromotionMapFilePath(String path);

		@Description("delimiter used to separate columns in the NCBITaxon promotion map file")
		@Required
		Delimiter getNcbiTaxonPromotionMapFileDelimiter();

		void setNcbiTaxonPromotionMapFileDelimiter(Delimiter delimiter);

		@Description("delimiter used to separate values in the 2nd column in the NCBITaxon promotion map file")
		@Required
		Delimiter getNcbiTaxonPromotionMapFileSetDelimiter();

		void setNcbiTaxonPromotionMapFileSetDelimiter(Delimiter delimiter);

		@Description("path to (pattern for) CRAFT extension class to OBO class mapping files are located")
		@Required
		String getExtensionMapFilePath();

		void setExtensionMapFilePath(String path);

		@Description("delimiter used to separate columns in the extension-to-obo class map file")
		@Required
		Delimiter getExtensionMapFileDelimiter();

		void setExtensionMapFileDelimiter(Delimiter delimiter);

		@Description("delimiter used to separate items in the set in the second column of the extension-to-obo class map file")
		@Required
		Delimiter getExtensionMapFileSetDelimiter();

		void setExtensionMapFileSetDelimiter(Delimiter delimiter);

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

		@Description("Allows user to specify whether concept annotations should be filtered by CRFs or not")
		@Required
		FilterFlag getFilterFlag();

		void setFilterFlag(FilterFlag value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

//		@Description("Should be either ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE or ProcessingStatusFlag.CONCEPT_POST_PROCESSING_UNFILTERED_DONE;")
//		ProcessingStatusFlag getTargetProcessingStatusFlag();
//
//		void setTargetProcessingStatusFlag(ProcessingStatusFlag value);

//		@Description("path to (pattern for) the file(s) containing mappings from ontology class to ancestor classes")
//		String getAncestorMapFilePath();
//
//		void setAncestorMapFilePath(String path);
//
//		@Description("delimiter used to separate columns in the ancestor map file")
//		Delimiter getAncestorMapFileDelimiter();
//
//		void setAncestorMapFileDelimiter(Delimiter delimiter);
//
//		@Description("delimiter used to separate items in the set in the second column of the ancestor map file")
//		Delimiter getAncestorMapFileSetDelimiter();
//
//		void setAncestorMapFileSetDelimiter(Delimiter delimiter);

	}

	public static void main(String[] args) {
//		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = null;
		if (options.getFilterFlag() == FilterFlag.BY_CRF) {
			targetProcessingStatusFlag = ProcessingStatusFlag.CONCEPT_POST_PROCESSING_DONE;
		} else {
			targetProcessingStatusFlag = ProcessingStatusFlag.CONCEPT_POST_PROCESSING_UNFILTERED_DONE;
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

		final PCollectionView<Map<String, Set<String>>> extensionToOboMapView = null;
		// we are not currenlty using extension ontology from CRAFT, so we comment this
		// out to reduce resource requirements
//		final PCollectionView<Map<String, Set<String>>> extensionToOboMapView = PCollectionUtil
//				.fromKeyToSetTwoColumnFiles("ext-to-obo map", p, options.getExtensionMapFilePath(),
//						options.getExtensionMapFileDelimiter(), options.getExtensionMapFileSetDelimiter(),
//						Compression.GZIP)
//				.apply(View.<String, Set<String>>asMap());

		final PCollectionView<Map<String, Set<String>>> ncbiTaxonPromotionMapView = null;
//		final PCollectionView<Map<String, Set<String>>> ncbiTaxonPromotionMapView = PCollectionUtil
//				.fromKeyToSetTwoColumnFiles("ncbitaxon promotion map", p, options.getNcbiTaxonPromotionMapFilePath(),
//						options.getNcbiTaxonPromotionMapFileDelimiter(),
//						options.getNcbiTaxonPromotionMapFileSetDelimiter(), Compression.GZIP)
//				.apply(View.<String, Set<String>>asMap());

		// pipeline was stalling when we brought in this map
//		final PCollectionView<Map<String, String>> idToOgerDictEntriesMapView = PCollectionUtil
//				.fromTwoColumnFiles("id to oger dict entry map part1", p, options.getIdToOgerDictEntryMapFilePath(),
//						options.getIdToOgerDictEntryMapFileDelimiter(), Compression.GZIP)
//				.apply(View.<String, String>asMap());

//		final PCollectionView<Map<String, Set<String>>> idToOgerDictEntriesMapView =
//				PCollectionUtil
//				.fromKeyToSetTwoColumnFiles("id to oger dict entry map", p, options.getIdToOgerDictEntryMapFilePath(),
//						options.getIdToOgerDictEntryMapFileDelimiter(),
//						options.getIdToOgerDictEntryMapFileSetDelimiter(), Compression.GZIP)
//				.apply(View.<String, Set<String>>asMap());

		DocumentType outputDocumentType = DocumentType.CONCEPT_ALL;
		if (options.getFilterFlag() == FilterFlag.NONE) {
			outputDocumentType = DocumentType.CONCEPT_ALL_UNFILTERED;
		}

		DocumentCriteria outputDocCriteria = new DocumentCriteria(outputDocumentType, DocumentFormat.BIONLP,
				PIPELINE_KEY, options.getOutputPipelineVersion());

		PCollectionTuple output = ConceptPostProcessingFn.process(statusEntity2Content, outputDocCriteria, timestamp,
				inputDocCriteria, extensionToOboMapView, ncbiTaxonPromotionMapView, options.getFilterFlag(),
				PIPELINE_KEY);

		PCollection<KV<ProcessingStatus, List<String>>> statusEntityToAnnotation = output
				.get(ConceptPostProcessingFn.ANNOTATIONS_TAG);
		PCollection<EtlFailureData> failures = output.get(ConceptPostProcessingFn.ETL_FAILURE_TAG);

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
