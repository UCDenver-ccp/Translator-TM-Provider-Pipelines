package edu.cuanschutz.ccp.tm_provider.etl.update;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.medline.PubmedArticle;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineStatusUpdateFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

/**
 * Originally designed to add the publication year and publication types to
 * Processing Status entities that already exist in Cloud Datastore.
 *
 */
public class UpdateMedlineEntitiesPipeline {

	public interface Options extends DataflowPipelineOptions {
		@Description("Path of the file to read from")
		String getMedlineXmlDir();

		void setMedlineXmlDir(String value);
	}

	public static void main(String[] args) {
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

		PCollection<KV<String, PubmedArticle>> docIdToArticle = keyByPubmedId(pubmedArticles);

		PCollection<KV<String, ProcessingStatus>> docIdToStatusEntity = PipelineMain.getStatusEntitiesToProcess(p,
				ProcessingStatusFlag.NOOP, CollectionsUtil.createSet(ProcessingStatusFlag.TEXT_DONE),
				options.getProject(), "PUBMED", OverwriteOutput.YES);

		/* group the lines by their ids */
		final TupleTag<PubmedArticle> articleTag = new TupleTag<>();
		final TupleTag<ProcessingStatus> statusEntityTag = new TupleTag<>();
		PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(articleTag, docIdToArticle)
				.and(statusEntityTag, docIdToStatusEntity).apply(CoGroupByKey.create());

		PCollectionTuple output = MedlineStatusUpdateFn.process(result, articleTag, statusEntityTag, timestamp);
		PCollection<EtlFailureData> failures = output.get(MedlineStatusUpdateFn.etlFailureTag);
		PCollection<ProcessingStatus> status = output.get(MedlineStatusUpdateFn.processingStatusTag);

		/*
		 * PipelineMain.updateStatusEntities() can update boolean flags, but when run
		 * without any flag parameters it simply converts the ProcessingStatus objects
		 * into Entity objects.
		 */
		PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(status);
		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

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

	/**
	 * @param pubmedArticles
	 * @return the input collection of PubmedArticles as a KV keyed by the PubMed ID
	 */
	public static PCollection<KV<String, PubmedArticle>> keyByPubmedId(PCollection<PubmedArticle> pubmedArticles) {
		return pubmedArticles.apply("create id/doc kv pair",
				ParDo.of(new DoFn<PubmedArticle, KV<String, PubmedArticle>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						PubmedArticle article = context.element();
						String pmid = "PMID:" + article.getMedlineCitation().getPMID().getvalue();
						context.output(KV.of(pmid, article));
					}
				}));
	}

}
