package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DependencyParseConlluToConll03Fn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * This pipeline takes as input the CoNNL-U Dependency Parse output of each
 * document and outputs tokens to file in the form of the first column of the
 * CoNLL 2003 file format, i.e., tokens in the order the appear with a blank
 * line between each sentence.
 */
public class DependencyParseToConll03Pipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.DEPENDENCY_PARSE_TO_CONLL03;

	public interface Options extends DataflowPipelineOptions {
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		@Required
		PipelineKey getTextPipelineKey();

		void setTextPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		@Required
		String getTextPipelineVersion();

		void setTextPipelineVersion(String value);

		@Description("The version of the dependency parse documents")
		@Required
		String getDpPipelineVersion();

		void setDpPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		@Required
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		ProcessingStatusFlag targetProcessingStatusFlag = ProcessingStatusFlag.CONLL03;
		// we require that the documents have a plain text version and dependency parse
		// output
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE,
				ProcessingStatusFlag.DP_DONE);

		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getTextPipelineKey(), options.getTextPipelineVersion());
		DocumentCriteria inputDpDocCriteria = new DocumentCriteria(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU,
				PipelineKey.DEPENDENCY_PARSE_IMPORT, options.getDpPipelineVersion());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(CollectionsUtil.createSet(inputTextDocCriteria, inputDpDocCriteria),
						options.getProject(), p, targetProcessingStatusFlag, requiredProcessStatusFlags,
						options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.TOKENS, DocumentFormat.CONLL03,
				PIPELINE_KEY, pipelineVersion);

		// extract sentences from the dependency parse & text files
		PCollectionTuple output = DependencyParseConlluToConll03Fn.process(statusEntity2Content, outputDocCriteria,
				timestamp);

		// key = documentId, value=text
		PCollection<KV<ProcessingStatus, String>> extractedText = output
				.get(DependencyParseConlluToConll03Fn.CONLL03_TAG);
		PCollection<EtlFailureData> failures = output.get(DependencyParseConlluToConll03Fn.ETL_FAILURE_TAG);

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
		nonredundantText.apply("write text", TextIO.write().to(options.getOutputBucket())
				.withCompression(Compression.GZIP).withSuffix("." + options.getCollection() + ".txt"));

		p.run().waitUntilFinish();
	}

}
