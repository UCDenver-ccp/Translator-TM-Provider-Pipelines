package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractContentFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

public class LoadFilesPipeline {

//	private final static Logger LOGGER = Logger.getLogger(LoadFilesPipeline.class.getName());

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		String getLoadDir();

		void setLoadDir(String value);

		@Description("File suffix to load")
		String getFileSuffix();

		void setFileSuffix(String value);

		@Description("The targetProcessingStatusFlag should align with the content of the file being loaded")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("Output document type, i.e. the document type that will be saved in Datastore")
		DocumentType getOutputDocumentType();

		void setOutputDocumentType(DocumentType value);

		@Description("Output document format, i.e. the document format that will be saved in Datastore")
		DocumentFormat getOutputDocumentFormat();

		void setOutputDocumentFormat(DocumentFormat value);

		@Description("Pipeline key")
		PipelineKey getPipelineKey();

		void setPipelineKey(PipelineKey value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		TypeDescriptor<KV<String, String>> td = new TypeDescriptor<KV<String, String>>() {
			private static final long serialVersionUID = 1L;
		};

		String filePattern = options.getLoadDir() + "/*" + options.getFileSuffix();
		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(filePattern))
				.apply(FileIO.readMatches()).apply(MapElements.into(td).via((ReadableFile f) -> {
					try {
						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
					} catch (IOException e) {
						throw new RuntimeException("Error while importing file content from file system.", e);
					}
				}));

		DocumentCriteria outputDocCriteria = new DocumentCriteria(options.getOutputDocumentType(),
				options.getOutputDocumentFormat(), options.getPipelineKey(), pipelineVersion);

		PCollectionTuple output = ExtractContentFn.process(fileIdAndContent, outputDocCriteria, timestamp,
				options.getFileSuffix(), options.getCollection(), options.getTargetProcessingStatusFlag());

		PCollection<KV<String, List<String>>> docIdToContent = output.get(ExtractContentFn.contentTag);
		PCollection<EtlFailureData> failures = output.get(ExtractContentFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(ExtractContentFn.processingStatusTag);

		/*
		 * store the processing status document for this pipeline in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> statusEntities = status.apply("status->status_entity",
				ParDo.of(new ProcessingStatusToEntityFn()));
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateEntitiesByKey(statusEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the document content in Cloud Datastore - deduplication is necessary to
		 * avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantContent = PipelineMain
				.deduplicateDocumentsByStringKey(docIdToContent);
		nonredundantContent
				.apply("content->document_entity",
						ParDo.of(new DocumentToEntityFn(outputDocCriteria, options.getCollection(),
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * if the target process is TEXT_DONE, then we don't need to update the status
		 * entity b/c a new one was created by the ExtractContentFn and then loaded
		 * above. However, if the target process is not TEXT_DONE, then the content
		 * being loaded from file is related to some other target process, presumably
		 * for documents that already exist in the Datastore, so we do need to update
		 * the status entites for the specified target process. For example, the file
		 * content might contain section annotations for documents already in the
		 * Datastore, so the target process would be {@link
		 * ProcessingStatusFlag.SECTIONS_DONE}.
		 */
		if (options.getTargetProcessingStatusFlag() != ProcessingStatusFlag.TEXT_DONE) {
			PCollection<KV<String, String>> successStatus = DatastoreProcessingStatusUtil.getSuccessStatus(
					docIdToContent.apply(Keys.<String>create()), failures, options.getTargetProcessingStatusFlag());

			List<PCollection<KV<String, String>>> statusList = new ArrayList<PCollection<KV<String, String>>>();
			statusList.add(successStatus);
			DatastoreProcessingStatusUtil.performStatusUpdatesInBatch(statusList);
		}

		p.run().waitUntilFinish();

	}

}
