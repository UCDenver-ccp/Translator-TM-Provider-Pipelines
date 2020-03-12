package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ToKVFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.TurkuDepParserFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.UpdateStatusFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * This Apache Beam pipeline processes documents with a dependency parser
 * reached via HTTP POST. Input is plain text; Output is CoNLL-U format.
 */
public class DependencyParsePipeline {

	public interface Options extends DataflowPipelineOptions {
		@Description("URI for the dependency parser service")
		String getDependencyParserServiceUri();

		void setDependencyParserServiceUri(String value);
	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		DatastoreProcessingStatusUtil statusUtil = new DatastoreProcessingStatusUtil();
		// we want to find documents that need dependency parsing
		ProcessingStatusFlag targetProcessStatusFlag = ProcessingStatusFlag.DP_DONE;
		// we require that the documents have a plain text version
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);
		// query Cloud Datastore to find document IDs in need of processing
		List<String> documentIdsToProcess = statusUtil.getDocumentIdsInNeedOfProcessing(targetProcessStatusFlag,
				requiredProcessStatusFlags);

		// query for the document content for those document IDs
		List<KV<String, String>> documentIdToContentList = new DatastoreDocumentUtil()
				.getDocumentIdToContentList(documentIdsToProcess, DocumentType.TEXT, DocumentFormat.TEXT);

		PCollection<KV<String, String>> docId2Content = p.apply(
				Create.of(documentIdToContentList).withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

		PCollectionTuple output = TurkuDepParserFn.process(docId2Content, options.getDependencyParserServiceUri());

		/*
		 * Processing of the plain text by the dependency parser results in 1) a
		 * PCollection mapping document ID to the CoNLL-U version of the document. 2) a
		 * PCollection logging any errors encountered during the dependency parse
		 * processing.
		 */

		PCollection<KV<String, String>> docIdToConllu = output.get(TurkuDepParserFn.CONLLU_TAG);
		PCollection<EtlFailureData> failures = output.get(TurkuDepParserFn.ETL_FAILURE_TAG);

		updateProcessingStatus(docIdToConllu, failures, ProcessingStatusFlag.DP_DONE);

		/* store the serialized CoNLL-U document content in Cloud Datastore */
		docIdToConllu
				.apply("conllu->document_entity",
						ParDo.of(new DocumentToEntityFn(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn(PipelineKey.DEPENDENCY_PARSE)))
				.apply("failure_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();
	}

	/**
	 * Logs the status of processing (set the flag=true) for the specified
	 * {@ProcessingStatusFlag} for documents that did not fail as true.
	 * 
	 * @param docIdToConllu
	 * @param failures
	 */
	private static void updateProcessingStatus(PCollection<KV<String, String>> docIdToConllu,
			PCollection<EtlFailureData> failures, ProcessingStatusFlag processingStatusFlag) {
		PCollection<String> failureDocIds = failures.apply("extract failure doc IDs",
				ParDo.of(new DoFn<EtlFailureData, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element EtlFailureData failure, OutputReceiver<String> out) {
						out.output(failure.getFileId());
					}

				}));

		PCollection<String> processedDocIds = docIdToConllu.apply(Keys.<String>create());
		PCollection<KV<String, Boolean>> processedDocIdsKV = processedDocIds.apply(ParDo.of(new ToKVFn()));
		PCollection<KV<String, Boolean>> failureDocIdsKV = failureDocIds.apply(ParDo.of(new ToKVFn()));

		// Merge collection values into a CoGbkResult collection.
		final TupleTag<Boolean> allTag = new TupleTag<Boolean>();
		final TupleTag<Boolean> failedTag = new TupleTag<Boolean>();
		PCollection<KV<String, CoGbkResult>> joinedCollection = KeyedPCollectionTuple.of(allTag, processedDocIdsKV)
				.and(failedTag, failureDocIdsKV).apply(CoGroupByKey.<String>create());

		joinedCollection.apply(ParDo.of(new UpdateStatusFn(processingStatusFlag, allTag, failedTag)));
	}

}
