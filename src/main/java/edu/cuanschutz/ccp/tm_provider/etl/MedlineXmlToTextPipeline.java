package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.medline.PubmedArticle;

import com.google.datastore.v1.Entity;

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
		@Default.String("gs://translator-tm-provider-datastore-staging-stage/medline2020/baseline")
		String getMedlineXmlDir();

		void setMedlineXmlDir(String value);

		@Description("The name of the document collection to process")
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
		String medlineXmlFilePattern = options.getMedlineXmlDir() + "/*.xml.gz";
//		PCollection<KV<String, String>> fileIdAndContent = p.apply(FileIO.match().filepattern(medlineXmlFilePattern))
//				.apply(FileIO.readMatches()).apply(MapElements.into(td).via((ReadableFile f) -> {
//					try {
//						return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
//					} catch (IOException e) {
//						throw new RuntimeException("Error while importing Medline XML files from file system.", e);
//					}
//				}));

		PCollection<ReadableFile> files = p.apply(FileIO.match().filepattern(medlineXmlFilePattern))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));

		PCollection<PubmedArticle> pubmedArticles = files
				.apply(XmlIO.<PubmedArticle>readFiles().withRootElement("PubmedArticleSet")
						.withRecordElement("PubmedArticle").withRecordClass(PubmedArticle.class));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PIPELINE_KEY, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, PIPELINE_KEY, pipelineVersion);

		PCollectionTuple output = MedlineXmlToTextFn.process(pubmedArticles, outputTextDocCriteria,
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

		/*
		 * store the plain text document content in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantPlainText = PipelineMain
				.deduplicateDocuments(docIdToPlainText);
		nonredundantPlainText
				.apply("plaintext->document_entity", ParDo.of(new DocumentToEntityFn(outputTextDocCriteria)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantAnnotations = PipelineMain
				.deduplicateDocuments(docIdToAnnotations);
		nonredundantAnnotations
				.apply("annotations->annot_entity", ParDo.of(new DocumentToEntityFn(outputAnnotationDocCriteria)))
				.apply("annot_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the failures for this pipeline in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntities(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the processing status document for this pipeline in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, Entity>> statusEntities = status.apply("status->status_entity",
				ParDo.of(new ProcessingStatusToEntityFn()));
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateEntities(statusEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();

	}

}
