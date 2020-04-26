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
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.OpenNLPSentenceSegmentFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * This Apache Beam pipeline processes documents with the OpenNLP sentence
 * segmenter. Input is plain text; Output is BioNLP format.
 */
public class SentenceSegmentationPipeline {

//	private final static Logger LOGGER = Logger.getLogger(DependencyParsePipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.SENTENCE_SEGMENTATION;

	public interface Options extends DataflowPipelineOptions {
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getInputPipelineKey();

		void setInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getInputPipelineVersion();

		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		// we want to find documents that need dependency parsing
		ProcessingStatusFlag targetProcessStatusFlag = ProcessingStatusFlag.SENTENCE_DONE;
		// we require that the documents have a plain text version
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getInputPipelineKey(), options.getInputPipelineVersion());

		PCollection<KV<String, String>> docId2Content = PipelineMain.getDocId2Content(inputTextDocCriteria,
				options.getProject(), p, targetProcessStatusFlag, requiredProcessStatusFlags, options.getCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);

		/* initialize to load the sentence segmentation model */
		PCollectionTuple output = OpenNLPSentenceSegmentFn.process(docId2Content, outputDocCriteria, timestamp);

		/*
		 * Processing of the plain text by the OpenNLP sentence segmenter 1) a
		 * PCollection mapping document ID to the BioNLP version of the sentence
		 * annotations. 2) a PCollection logging any errors encountered during sentence
		 * segmentation processing.
		 */

		PCollection<KV<String, List<String>>> docIdToSentenceBioNLP = output
				.get(OpenNLPSentenceSegmentFn.SENTENCE_ANNOT_TAG);
		PCollection<EtlFailureData> failures = output.get(OpenNLPSentenceSegmentFn.ETL_FAILURE_TAG);

		/* store the serialized CoNLL-U document content in Cloud Datastore */
		docIdToSentenceBioNLP.apply("conllu->document_entity", ParDo.of(new DocumentToEntityFn(outputDocCriteria)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn())).apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// update the status for documents that were successfully processed
		PCollection<KV<String, String>> successStatus = DatastoreProcessingStatusUtil.getSuccessStatus(
				docId2Content.apply(Keys.<String>create()), failures, ProcessingStatusFlag.SENTENCE_DONE);
		List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
		statusList.add(successStatus);
		DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);

		p.run().waitUntilFinish();
	}

}
