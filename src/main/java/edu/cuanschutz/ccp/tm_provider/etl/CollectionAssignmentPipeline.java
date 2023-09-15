package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
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

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
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

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		DocumentType getInputDocumentType();

		void setInputDocumentType(DocumentType value);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		DocumentFormat getInputDocumentFormat();

		void setInputDocumentFormat(DocumentFormat value);

		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getInputPipelineKey();

		void setInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getInputPipelineVersion();

		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		String getInputCollection();

		void setInputCollection(String value);

		@Description("The document collection to process")
		String getOutputCollection();

		void setOutputCollection(String value);

		@Description("The document collection to process")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		String inputCollection = options.getInputCollection();
		String project = options.getProject();
		String outputCollection = options.getOutputCollection();

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version -- this is the default
		// requirement
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		/* initialize the document criteria that we are searching for */
		DocumentCriteria inputDocCriteria = new DocumentCriteria(options.getInputDocumentType(),
				options.getInputDocumentFormat(), options.getInputPipelineKey(), options.getInputPipelineVersion());

		/*
		 * create a collection of document identifiers that have a corresponding
		 * document that matches the inputDocCriteria
		 */
		PCollection<String> observedDocumentIdsMatchingDocCriteria = PipelineMain.getDocumentIdsForExistingDocuments(p,
				inputDocCriteria, inputCollection, project);

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
						if (processingStatus != null && docIdMissingDoc != null && docIdMissingDoc.iterator().hasNext()) {
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
