package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
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

		@Description("Output document type, i.e. the document type that will be saved in Datastore")
		DocumentType getOutputDocumentType();

		void setOutputDocumentType(DocumentType value);

		@Description("Output document format, i.e. the document format that will be saved in Datastore")
		DocumentFormat getOutputDocumentFormat();

		void setOutputDocumentFormat(DocumentFormat value);

		@Description("Pipeline key")
		PipelineKey getPipelineKey();

		void setPipelineKey(PipelineKey value);

		@Description("Document collection name")
		String getCollectionName();

		void setCollectionName(String value);

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
						throw new RuntimeException("Error while importing BioC XML files from file system.", e);
					}
				}));

		DocumentCriteria outputDocCriteria = new DocumentCriteria(options.getOutputDocumentType(),
				options.getOutputDocumentFormat(), options.getPipelineKey(), pipelineVersion);

		PCollectionTuple output = ExtractTextFn.process(fileIdAndContent, outputDocCriteria, timestamp,
				options.getFileSuffix(), options.getCollectionName());

		PCollection<KV<String, List<String>>> docIdToPlainText = output.get(ExtractTextFn.plainTextTag);
		PCollection<EtlFailureData> failures = output.get(ExtractTextFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(ExtractTextFn.processingStatusTag);

		/* store the bioc document content in Cloud Datastore */
		/* store the plain text document content in Cloud Datastore */
		docIdToPlainText.apply("plaintext->document_entity", ParDo.of(new DocumentToEntityFn(outputDocCriteria)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn())).apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the processing status document for this pipeline in Cloud Datastore */
		status.apply("status->status_entity", ParDo.of(new ProcessingStatusToEntityFn()))
				.apply("status_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();

	}

}
