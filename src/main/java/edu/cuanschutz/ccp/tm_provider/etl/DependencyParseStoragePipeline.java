package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.BiocToTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DependencyParseImportFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.SerializableFunction;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Consumes Dependency Parse CONLL-U files that contain multiple documents,
 * parses them into individual documents, then stores the CONLL-U in Cloud
 * Datastore
 */
public class DependencyParseStoragePipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.DEPENDENCY_PARSE_IMPORT;

	public interface Options extends DataflowPipelineOptions {
		@Description("GCS path to the CONLL-U files to load - this is the base path with the next directory expected to be the collection name.")
		String getBaseDependencyParseFilePath();

		void setBaseDependencyParseFilePath(String path);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous CONLL-U data")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);
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
//		String biocFilePattern = options.getBaseDependencyParseFilePath() + "/*.dependeny_parses.conllu.gz";
//		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(biocFilePattern))
//				.apply(FileIO.readMatches().withCompression(Compression.GZIP))
//				.apply(MapElements.into(td).via((ReadableFile f) -> {
//					try {
//						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
//					} catch (IOException e) {
//						throw new RuntimeException("Error while importing BioC XML files from file system.", e);
//					}
//				}));

		String dependencyParseFilePattern = options.getBaseDependencyParseFilePath() + "/*.dependeny_parses.conllu.gz";
//		PCollection<ReadableFile> files = p
//				.apply("get CONLL-U files to load", FileIO.match().filepattern(dependencyParseFilePattern))
//				.apply(FileIO.readMatches().withCompression(Compression.GZIP));

		PCollection<KV<String, String>> fileIdAndContent = p
				.apply(FileIO.match().filepattern(dependencyParseFilePattern)).apply(FileIO.readMatches())
				.apply(MapElements.into(td).via((ReadableFile f) -> {
					try {
						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
					} catch (IOException e) {
						throw new RuntimeException(
								"Error while importing bulk dependency parse CONLLU files from file system.", e);
					}
				}));

		DocumentCriteria conlluDocCriteria = new DocumentCriteria(DocumentType.DEPENDENCY_PARSE, DocumentFormat.CONLLU,
				PIPELINE_KEY, pipelineVersion);
		DocumentCriteria sentenceDocCriteria = new DocumentCriteria(DocumentType.SENTENCE, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = DependencyParseImportFn.process(fileIdAndContent, conlluDocCriteria, timestamp);

		
		code up output for conllu and sentence -- and update the status document?
		
		/*
		 * Processing of the BioC XML documents resulted in at least two, and possibly
		 * three PCollections. 1) a PCollection mapping document ID to the plain text
		 * version of the document. 2) a PCollection mapping document ID to a String
		 * representation (BioNLP format) of the section annotations in the document,
		 * and (possibly) 3) a PCollection logging any errors encountered during the
		 * BioC-->Plain-text conversion. 4) a PCollection logging the processing status
		 * of each document
		 */

		PCollection<KV<String, List<String>>> docIdToPlainText = output.get(BiocToTextFn.plainTextTag);
		PCollection<KV<String, List<String>>> docIdToAnnotations = output.get(BiocToTextFn.sectionAnnotationsTag);
		PCollection<EtlFailureData> failures = output.get(BiocToTextFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(BiocToTextFn.processingStatusTag);

		SerializableFunction<String, String> collectionFn = parameter -> getSubCollectionName(parameter);

		/*
		 * store the processing status document for this pipeline in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> statusEntities = status.apply("status->status_entity",
				ParDo.of(new ProcessingStatusToEntityFn(collectionFn)));
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateEntitiesByKey(statusEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		PCollectionView<Map<String, Set<String>>> documentIdToCollections = PipelineMain
				.getCollectionMappings(nonredundantStatusEntities).apply(View.<String, Set<String>>asMap());

		/*
		 * store the plain text document content in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantPlainText = PipelineMain
				.deduplicateDocumentsByStringKey(docIdToPlainText);
		nonredundantPlainText
				.apply("plaintext->document_entity",
						ParDo.of(new DocumentToEntityFn(outputTextDocCriteria, options.getCollection(), collectionFn,
								documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantAnnotations = PipelineMain
				.deduplicateDocumentsByStringKey(docIdToAnnotations);
		nonredundantAnnotations
				.apply("annotations->annot_entity",
						ParDo.of(new DocumentToEntityFn(outputAnnotationDocCriteria, options.getCollection(),
								collectionFn, documentIdToCollections)).withSideInputs(documentIdToCollections))
				.apply("annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();
	}

}
