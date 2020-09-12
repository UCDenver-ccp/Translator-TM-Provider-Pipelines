package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractedSentence;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SentenceExtractionFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SentenceTsvBuilderFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Useful for extracting sentences for creating corpora for manual annotation
 */
public class SentenceExtractionPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.SENTENCE_EXTRACTION;

	public interface Options extends DataflowPipelineOptions {
		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("Keywords to include in the sentences that are extracted. Input is a pipe-delimited list of keywords")
		String getKeywords();

		void setKeywords(String keywords);

//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentType getTargetDocumentType();
//
//		void setTargetDocumentType(DocumentType type);
//
//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentFormat getTargetDocumentFormat();
//
//		void setTargetDocumentFormat(DocumentFormat type);
//
//		@Description("This pipeline key will be used to select the input text documents that will be processed")
//		PipelineKey getInputPipelineKey();
//
//		void setInputPipelineKey(PipelineKey value);
//
//		@Description("This pipeline version will be used to select the input text documents that will be processed")
//		String getInputPipelineVersion();
//
//		void setInputPipelineVersion(String value);

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

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());
		Set<String> keywords = compileKeywords(options.getKeywords());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);
		PCollectionTuple output = SentenceExtractionFn.process(statusEntity2Content, keywords, outputDocCriteria,
				timestamp, inputDocCriteria);

		PCollection<KV<ProcessingStatus, ExtractedSentence>> extractedSentences = output
				.get(SentenceExtractionFn.EXTRACTED_SENTENCES_TAG);
		PCollection<EtlFailureData> failures = output.get(SentenceExtractionFn.ETL_FAILURE_TAG);

		/*
		 * store failures from sentence extraction
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ext failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// de-duplication of extracted sentences?
		output = SentenceTsvBuilderFn.process(extractedSentences, outputDocCriteria, timestamp);

		PCollection<KV<ProcessingStatus, String>> statusToOutputTsv = output.get(SentenceTsvBuilderFn.OUTPUT_TSV_TAG);
		failures = output.get(SentenceTsvBuilderFn.ETL_FAILURE_TAG);

		/*
		 * store failures from output file format creation
		 */
		failureEntities = failures.apply("tsv failures->datastore", ParDo.of(new EtlFailureToEntityFn()));
		nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
				statusToOutputTsv.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// output sentences to file
		PCollection<String> outputTsv = statusToOutputTsv
				.apply(ParDo.of(new DoFn<KV<ProcessingStatus, String>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<ProcessingStatus, String> element = c.element();
						c.output(element.getValue());
					}
				}));
		outputTsv.apply("write annotation table", TextIO.write().to(options.getOutputBucket()).withSuffix(".tsv"));

		p.run().waitUntilFinish();
	}

	@VisibleForTesting
	protected static Set<String> compileKeywords(String keywords) {
		if (keywords == null || keywords.isEmpty()) {
			return new HashSet<String>();
		}
		return new HashSet<String>(Arrays.asList(keywords.split("\\|")));
	}

}
