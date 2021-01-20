package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
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

import edu.cuanschutz.ccp.tm_provider.etl.MedlineXmlToTextPipeline.UniqueStrings;
import edu.cuanschutz.ccp.tm_provider.etl.fn.BiocToTextFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ProcessingStatusToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.SerializableFunction;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class BiocToTextPipeline {

//	private final static Logger LOGGER = Logger.getLogger(BigQueryExportPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.BIOC_TO_TEXT;

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://tm-provider-dataflow-work/bioc-downloads/*")
		String getBiocDir();

		void setBiocDir(String value);

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

		// https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/sdk/io/FileIO.html
		String biocFilePattern = options.getBiocDir() + "/*.xml";
		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(biocFilePattern))
				.apply(FileIO.readMatches()).apply(MapElements.into(td).via((ReadableFile f) -> {
					try {
						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
					} catch (IOException e) {
						throw new RuntimeException("Error while importing BioC XML files from file system.", e);
					}
				}));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PIPELINE_KEY, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, PIPELINE_KEY, pipelineVersion);

		// catalog documents that have already been stored so that we can exclude
		// loading redundant documents. There will be cases in the PubMed/Medline update
		// files where records are included due to changes, e.g. modification date, but
		// are not new documents, so they should be excluded from being loaded into
		// datastore here.

		PCollection<KV<String, ProcessingStatus>> docIdToStatusEntity = PipelineMain.getStatusEntitiesToProcess(p,
				ProcessingStatusFlag.NOOP, CollectionsUtil.createSet(ProcessingStatusFlag.TEXT_DONE),
				options.getProject(), options.getCollection(), OverwriteOutput.YES);

		// create a set of document IDs already present in Datastore to be used as a
		// side input
		PCollection<String> docIds = docIdToStatusEntity.apply(Keys.<String>create());
		PCollection<Set<String>> docIdsSet = docIds.apply(Combine.globally(new UniqueStrings()));
		final PCollectionView<Set<String>> existingDocumentIds = docIdsSet.apply(View.<Set<String>>asSingleton());

		PCollectionTuple output = BiocToTextFn.process(fileIdAndContent, outputTextDocCriteria,
				outputAnnotationDocCriteria, timestamp, options.getCollection(), existingDocumentIds);

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
		 * store the plain text document content in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantPlainText = PipelineMain
				.deduplicateDocumentsByStringKey(docIdToPlainText);
		nonredundantPlainText
				.apply("plaintext->document_entity",
						ParDo.of(new DocumentToEntityFn(outputTextDocCriteria, options.getCollection(), collectionFn)))
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
								collectionFn)))
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

		/*
		 * store the processing status document for this pipeline in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> statusEntities = status.apply("status->status_entity",
				ParDo.of(new ProcessingStatusToEntityFn(collectionFn)));
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateEntitiesByKey(statusEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();

	}

	protected static String getSubCollectionName(String documentId) {
		try {
			if (documentId.startsWith("PMC")) {
				int idAsNum = Integer.parseInt(documentId.substring(3));
				int bin = Math.floorDiv(idAsNum, 250000);
				return String.format("PMC_SUBSET_%d", bin);
			}
			return null;
		} catch (NumberFormatException nfe) {
			return null;
		}
	}
}
