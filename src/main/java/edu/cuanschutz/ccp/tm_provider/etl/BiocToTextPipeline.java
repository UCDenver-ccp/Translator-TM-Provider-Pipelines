package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;

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

import edu.cuanschutz.ccp.tm_provider.etl.fn.BiocToTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;

public class BiocToTextPipeline {

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://tm-provider-dataflow-work/bioc-downloads/*")
		String getBiocDir();

		void setBiocDir(String value);

	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		TypeDescriptor<KV<String, String>> td = new TypeDescriptor<KV<String, String>>() {
			private static final long serialVersionUID = 1L;
		};

		// https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/sdk/io/FileIO.html
		/* Note: bioc files will be downloaded as xml */
		String biocFilePattern = options.getBiocDir() + "/*.xml";
		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(biocFilePattern))
				.apply(FileIO.readMatches()).apply(MapElements.into(td).via((ReadableFile f) -> {
					try {
						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
					} catch (IOException e) {
						throw new RuntimeException("Error while importing BioC XML files from file system.", e);
					}
				}));

		PCollectionTuple output = BiocToTextFn.process(fileIdAndContent);

		/*
		 * Processing of the BioC XML documents resulted in at least two, and possibly
		 * three PCollections. 1) a PCollection mapping document ID to the plain text
		 * version of the document. 2) a PCollection mapping document ID to a String
		 * representation (BioNLP format) of the section annotations in the document,
		 * and (possibly) 3) a PCollection logging any errors encountered during the
		 * BioC-->Plain-text conversion. 4) a PCollection logging the processing status
		 * of each document
		 */

		PCollection<KV<String, String>> docIdToPlainText = output.get(BiocToTextFn.plainTextTag);
		PCollection<KV<String, String>> docIdToAnnotations = output.get(BiocToTextFn.sectionAnnotationsTag);
		PCollection<EtlFailureData> failures = output.get(BiocToTextFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(BiocToTextFn.processingStatusTag);

		/* store the bioc document content in Cloud Datastore */
		fileIdAndContent
				.apply("biocxml->document_entity",
						ParDo.of(new DocumentToEntityFn(DocumentType.BIOC, DocumentFormat.BIOCXML)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the plain text document content in Cloud Datastore */
		docIdToPlainText
				.apply("plaintext->document_entity",
						ParDo.of(new DocumentToEntityFn(DocumentType.TEXT, DocumentFormat.TEXT)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the serialized annotation document content in Cloud Datastore */
		docIdToAnnotations
				.apply("annotations->document_entity",
						ParDo.of(new DocumentToEntityFn(DocumentType.SECTIONS, DocumentFormat.BIONLP)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the failures for this pipeline in Cloud Datastore */
		failures.apply("failures->datastore", ParDo.of(new EtlFailureToEntityFn(PipelineKey.BIOC_TO_TEXT)))
				.apply("failure_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/* store the processing status document for this pipeline in Cloud Datastore */
		status.apply("status->status_entity", ParDo.of(new ProcessingStatusToEntityFn()))
				.apply("status_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();

	}

}
