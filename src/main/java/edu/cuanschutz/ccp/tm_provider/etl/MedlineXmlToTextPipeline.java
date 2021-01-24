package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.tools.ant.util.StringUtils;
import org.medline.PubmedArticle;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn;
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

public class MedlineXmlToTextPipeline {

//	private final static Logger LOGGER = Logger.getLogger(BigQueryExportPipeline.class.getName());

	private static final PipelineKey PIPELINE_KEY = PipelineKey.MEDLINE_XML_TO_TEXT;
	private static final String SUBCOLLECTION_PREFIX = "PUBMED_SUB_";

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://translator-tm-provider-datastore-staging-stage/medline2020/baseline")
		String getMedlineXmlDir();

		void setMedlineXmlDir(String value);

		@Description("The name of the document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Overwrite any previous imported documents")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);
	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		// https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/sdk/io/FileIO.html
		String medlineXmlFilePattern = options.getMedlineXmlDir() + "/*.xml.gz";
		PCollection<ReadableFile> files = p.apply(FileIO.match().filepattern(medlineXmlFilePattern))
				.apply(FileIO.readMatches().withCompression(Compression.GZIP));

		PCollection<PubmedArticle> pubmedArticles = files
				.apply(XmlIO.<PubmedArticle>readFiles().withRootElement("PubmedArticleSet")
						.withRecordElement("PubmedArticle").withRecordClass(PubmedArticle.class));

		DocumentCriteria outputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				PIPELINE_KEY, pipelineVersion);
		DocumentCriteria outputAnnotationDocCriteria = new DocumentCriteria(DocumentType.SECTIONS,
				DocumentFormat.BIONLP, PIPELINE_KEY, pipelineVersion);

		final PCollectionView<Set<String>> docIdsSetView = PipelineMain.catalogExistingDocuments(options.getProject(),
				options.getCollection(), options.getOverwrite(), p);

		PCollectionTuple output = MedlineXmlToTextFn.process(pubmedArticles, outputTextDocCriteria, timestamp,
				options.getCollection(), docIdsSetView, options.getOverwrite());

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

		PCollection<KV<String, List<String>>> statusEntityToPlainText = output.get(MedlineXmlToTextFn.plainTextTag);
		PCollection<KV<String, List<String>>> statusEntityToAnnotation = output
				.get(MedlineXmlToTextFn.sectionAnnotationsTag);
		PCollection<EtlFailureData> failures = output.get(MedlineXmlToTextFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(MedlineXmlToTextFn.processingStatusTag);

		/*
		 * We want to add a subcollection to divide up the PubMed documents. The
		 * subcollection names are based on the PMIDs of the documents.
		 */
		SerializableFunction<String, String> collectionFn = parameter -> getSubCollectionName(parameter);

		/*
		 * store the plain text document content in Cloud Datastore - deduplication is
		 * necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantPlainText = PipelineMain
				.deduplicateDocumentsByStringKey(statusEntityToPlainText);
		nonredundantPlainText
				.apply("plaintext->document_entity",
						ParDo.of(new DocumentToEntityFn(outputTextDocCriteria, options.getCollection(), collectionFn)))
				.apply("document_entity->datastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * store the serialized annotation document content in Cloud Datastore -
		 * deduplication is necessary to avoid Datastore non-transactional commit errors
		 */
		PCollection<KV<String, List<String>>> nonredundantStatusEntityToAnnotations = PipelineMain
				.deduplicateDocumentsByStringKey(statusEntityToAnnotation);
		nonredundantStatusEntityToAnnotations
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

	private static String getSubCollectionName(String documentId) {
		if (documentId.startsWith("PMID:")) {
			int pmid = Integer.parseInt(StringUtils.removePrefix(documentId, "PMID:"));

			int windowSize = 1000000;
			int subCollectionIndex = 0;
			while (true) {
				int windowStart = subCollectionIndex * windowSize;
				int windowEnd = windowStart + windowSize;
				if (pmid >= windowStart && pmid < windowEnd) {
					return String.format("%s%d", SUBCOLLECTION_PREFIX, subCollectionIndex);
				}
				subCollectionIndex++;
			}
		}
		return null;
	}

	/** Modeled after code in og.apache.beam.sdk.transforms.CombineTest */
	public static class UniqueStrings extends Combine.CombineFn<String, Set<String>, Set<String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Set<String> createAccumulator() {
			return new HashSet<>();
		}

		@Override
		public Set<String> addInput(Set<String> accumulator, String input) {
			accumulator.add(input);
			return accumulator;
		}

		@Override
		public Set<String> mergeAccumulators(Iterable<Set<String>> accumulators) {
			Set<String> all = new HashSet<>();
			for (Set<String> part : accumulators) {
				all.addAll(part);
			}
			return all;
		}

		@Override
		public Set<String> extractOutput(Set<String> accumulator) {
			return accumulator;
		}
	}

}
