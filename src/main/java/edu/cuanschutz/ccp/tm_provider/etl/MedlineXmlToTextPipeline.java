package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
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
import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

public class MedlineXmlToTextPipeline {

//	private final static Logger LOGGER = Logger.getLogger(BigQueryExportPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.MEDLINE_XML_TO_TEXT;

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://translator-tm-provider-datastore-staging-stage/medline2020/baseline/*")
		String getMedlineXmlDir();

		void setMedlineXmlDir(String value);

		@Description("The name of the document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("The base file path (in bucket) where the text files will be stored.")
		String getTextOutputPath();

		void setTextOutputPath(String value);

		@Description("The base file path (in bucket) where the annotation files will be stored.")
		String getAnnotationOutputPath();

		void setAnnotationOutputPath(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		TypeDescriptor<KV<String, String>> td = new TypeDescriptor<KV<String, String>>() {
			private static final long serialVersionUID = 1L;
		};

		// https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/sdk/io/FileIO.html
		String medlineXmlFilePattern = options.getMedlineXmlDir() + "/*.xml.gz";
		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(medlineXmlFilePattern))
				.apply(FileIO.readMatches()).apply(MapElements.into(td).via((ReadableFile f) -> {
					try {
						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
					} catch (IOException e) {
						throw new RuntimeException("Error while importing Medline XML files from file system.", e);
					}
				}));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PIPELINE_KEY, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = MedlineXmlToTextFn.process(fileIdAndContent, outputTextDocCriteria,
				outputAnnotationDocCriteria, timestamp, options.getCollection());

		/*
		 * Processing of the Medline XML documents resulted in at least two, and
		 * possibly three PCollections. 1) a PCollection mapping document ID to the
		 * plain text version of the document. 2) a PCollection mapping document ID to a
		 * String representation (BioNLP format) of the section annotations in the
		 * document; for Medline documents there will be two sections (title &
		 * abstract), and (possibly) 3) a PCollection logging any errors encountered
		 * during the MedlineXML-->Plain-text conversion. 4) a PCollection logging the
		 * processing status of each document
		 */

		PCollection<KV<String, List<String>>> docIdToPlainText = output.get(MedlineXmlToTextFn.plainTextTag);
		PCollection<KV<String, List<String>>> docIdToAnnotations = output.get(MedlineXmlToTextFn.sectionAnnotationsTag);
		PCollection<EtlFailureData> failures = output.get(MedlineXmlToTextFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(MedlineXmlToTextFn.processingStatusTag);

		/* store the plain text document content in Cloud Datastore */
		docIdToPlainText.apply("plaintext->document_entity", ParDo.of(new DocumentToEntityFn(outputTextDocCriteria)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the serialized annotation document content in Cloud Datastore */
		docIdToAnnotations
				.apply("annotations->document_entity", ParDo.of(new DocumentToEntityFn(outputAnnotationDocCriteria)))
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