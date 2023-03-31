package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.TextExtractionFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Originally written to write text to files to be further processed downstream
 * by the Turku dependency parser. The text of each document is prepended with a
 * comment field "###C: [document ID]" that contains the document ID so that the
 * output of the dependency parser can be segmented into document-specific
 * chunks.
 */
public class TextExtractionPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.TEXT_EXTRACTION;

	public static final String COMMENT_INDICATOR = "###C: ";

	public interface Options extends DataflowPipelineOptions {
		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.TEXT_EXTRACTION;
		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		// the output document criteria is used primarily to populate error messages in
		// case of pipeline failures
		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT, PIPELINE_KEY,
				pipelineVersion);

		PCollectionTuple output = TextExtractionFn.process(statusEntity2Content, outputDocCriteria, timestamp,
				inputDocCriteria);

		// key = documentId, value=text
		PCollection<KV<ProcessingStatus, String>> extractedText = output.get(TextExtractionFn.EXTRACTED_TEXT_TAG);
		PCollection<EtlFailureData> failures = output.get(TextExtractionFn.ETL_FAILURE_TAG);

		/*
		 * store failures from text extraction
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ext failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollection<KV<String, String>> nonredundantIdToText = PipelineMain.deduplicateDocuments(extractedText);

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain
				.updateStatusEntities(extractedText.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollection<String> nonredundantText = nonredundantIdToText
				.apply(ParDo.of(new DoFn<KV<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, String> element = c.element();
						c.output(element.getValue());
					}
				}));
		nonredundantText.apply("write text",
				TextIO.write().to(options.getOutputBucket()).withSuffix("." + options.getCollection() + ".txt"));

		p.run().waitUntilFinish();
	}

}
