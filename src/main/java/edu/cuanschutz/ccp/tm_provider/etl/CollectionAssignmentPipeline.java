package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sets;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * Originally written to write text to files to be further processed downstream
 * by the Turku dependency parser. The text of each document is prepended with a
 * comment field "###C: DOCUMENT_ID\t[document ID]" that contains the document
 * ID so that the output of the dependency parser can be segmented into
 * document-specific chunks. The text is also prepended with a comment field
 * listing the document collections to which the document belongs, e.g. <br>
 * ###C: DOCUMENT_COLLECTIONS\tcollection1|collection2|collection3
 *
 */
public class CollectionAssignmentPipeline {

	private static final String MISSING_TAG = "missing";
	private static final String STATUS_TAG = "status";
	private static final PipelineKey PIPELINE_KEY = PipelineKey.COLLECTION_ASSIGNMENT;

	public interface Options extends DataflowPipelineOptions {

		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		@Required
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

//		@Description("This pipeline key will be used to select the input text documents that will be processed")
//		@Required
//		DocumentType getInputDocumentType();
//
//		void setInputDocumentType(DocumentType value);
//
//		@Description("This pipeline key will be used to select the input text documents that will be processed")
//		@Required
//		DocumentFormat getInputDocumentFormat();
//
//		void setInputDocumentFormat(DocumentFormat value);
//
//		@Description("This pipeline key will be used to select the input text documents that will be processed")
//		@Required
//		PipelineKey getInputPipelineKey();
//
//		void setInputPipelineKey(PipelineKey value);
//
//		@Description("This pipeline version will be used to select the input text documents that will be processed")
//		@Required
//		String getInputPipelineVersion();
//
//		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getInputCollection();

		void setInputCollection(String value);

		@Description("The document collection to process")
		@Required
		String getOutputCollection();

		void setOutputCollection(String value);

		@Description("The document collection to process")
		@Required
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		String inputCollection = options.getInputCollection();
		String project = options.getProject();
		String outputCollection = options.getOutputCollection();
		String optionalDocumentSpecificCollection = options.getOptionalDocumentSpecificCollection();

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version -- this is the default
		// requirement
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		/* initialize the document criteria that we are searching for */
		List<DocumentCriteria> inputDocCriterias = new ArrayList<DocumentCriteria>(
				PipelineMain.compileInputDocumentCriteria(options.getInputDocumentCriteria()));

//		DocumentCriteria inputDocCriteria = new DocumentCriteria(options.getInputDocumentType(),
//				options.getInputDocumentFormat(), options.getInputPipelineKey(), options.getInputPipelineVersion());

		/*
		 * create a collection of document identifiers that have a corresponding
		 * document that matches the inputDocCriteria
		 */
		String documentCollection = inputCollection;
		if (optionalDocumentSpecificCollection != null) {
			documentCollection = optionalDocumentSpecificCollection;
		}

		PCollectionList<String> observedDocumentIDsMatchingDocCriterias = PCollectionList.of(PipelineMain
				.getDocumentIdsForExistingDocuments(p, inputDocCriterias.get(0), documentCollection, project));
		for (int i = 1; i < inputDocCriterias.size(); i++) {
			observedDocumentIDsMatchingDocCriterias = observedDocumentIDsMatchingDocCriterias.and(PipelineMain
					.getDocumentIdsForExistingDocuments(p, inputDocCriterias.get(i), documentCollection, project));
		}

		/* take the intersection of all observed-document-ids-matching-doc-criteria */
		PCollection<String> observedDocumentIdsMatchingDocCriteria = observedDocumentIDsMatchingDocCriterias
				.apply("intersection", Sets.intersectAll());

		/* retrieve all ProcessingStatus objects for the specified collection */
		PCollection<KV<String, ProcessingStatus>> processingStatusForCollection = PipelineMain
				.getStatusEntitiesToProcess(p, ProcessingStatusFlag.NOOP, requiredProcessStatusFlags, project,
						inputCollection, OverwriteOutput.YES);

		/*
		 * find document identifiers that are not associated with a document that
		 * matches the input document criteria
		 */
		PCollection<String> docIdsInCollection = processingStatusForCollection.apply(Keys.<String>create());
		PCollection<String> docIdsMissingDocument = docIdsInCollection.apply("set diff",
				Sets.exceptAll(observedDocumentIdsMatchingDocCriteria));

		// There must be a better way to filter based on document IDs. I'm not sure how
		// else to do it, so we'll create a map from doc id to doc id so that we can use
		// the Combine method below.
		PCollection<KV<String, String>> missingDocIdToDocId = docIdsMissingDocument.apply("create map to self",
				ParDo.of(new DoFn<String, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element String id, OutputReceiver<KV<String, String>> out) {
						out.output(KV.of(id, id));
					}
				}));

		KeyedPCollectionTuple<String> tuple = KeyedPCollectionTuple.of(STATUS_TAG, processingStatusForCollection)
				.and(MISSING_TAG, missingDocIdToDocId);

		PCollection<KV<String, CoGbkResult>> result = tuple.apply("merge status entities with docs",
				CoGroupByKey.create());

		PCollection<Entity> updatedEntities = result.apply("assign collection to status entity",
				ParDo.of(new DoFn<KV<String, CoGbkResult>, Entity>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> element = c.element();
						CoGbkResult result = element.getValue();

						// get the processing status -- there will only be one
						ProcessingStatus processingStatus = result.getOnly(STATUS_TAG);
						Iterable<Object> docIdMissingDoc = result.getAll(MISSING_TAG);
						if (processingStatus != null && docIdMissingDoc != null
								&& docIdMissingDoc.iterator().hasNext()) {
							processingStatus.addCollection(outputCollection);
							processingStatus.disableFlag(targetProcessingStatusFlag);
							// we are using the updateStatusEntity method simply to convert ProcessingStatus
							// to an Entity, hence the null flags
							Entity updatedEntity = PipelineMain.updateStatusEntity(processingStatus,
									(ProcessingStatusFlag[]) null);
							c.output(updatedEntity);
						}
					}

				}));

		/* update the status entities that were assigned a new collection */
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore", DatastoreIO.v1().write().withProjectId(project));

		p.run().waitUntilFinish();
	}

}
