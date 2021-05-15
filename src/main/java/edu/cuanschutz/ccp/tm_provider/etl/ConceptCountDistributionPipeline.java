package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;

/**
 * Makes use of some of the Normalized Google Distance machinery to compute
 * concept count distributions across documents
 */
public class ConceptCountDistributionPipeline {

	public interface Options extends DataflowPipelineOptions {

		@Description("File pattern to match the files containing concept-id/document-id pairs")
		String getSingletonFilePattern();

		void setSingletonFilePattern(String filePattern);

		@Description("File pattern to match the files containing concept-id to label pairs")
		String getLabelMapFilePattern();

		void setLabelMapFilePattern(String filePattern);

		@Description("Delimiter used in the id-to-label file")
		Delimiter getLabelMapFileDelimiter();

		void setLabelMapFileDelimiter(Delimiter delimiter);

		@Description("Path to the bucket where results will be written")
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		final PCollection<KV<String, Long>> conceptIdToCounts = NgdKgxPipeline
				.getSingletonCountMapView(options.getSingletonFilePattern(), p);

		final PCollectionView<Map<String, String>> conceptIdToLabelMap = PCollectionUtil.fromTwoColumnFiles(p,
				options.getLabelMapFilePattern(), options.getLabelMapFileDelimiter(), Compression.GZIP)
				.apply(View.<String, String>asMap());

		// create concept-to-document count lines
		PCollection<String> lines = createConceptToDocumentCountLines(conceptIdToCounts, conceptIdToLabelMap);

		// output lines to file
		lines.apply("write lines to file", TextIO.write().to(options.getOutputBucket()).withSuffix(".tsv"));

		p.run().waitUntilFinish();
	}

	private static PCollection<String> createConceptToDocumentCountLines(
			final PCollection<KV<String, Long>> conceptIdToCounts,
			final PCollectionView<Map<String, String>> conceptIdToLabelMap) {

		PCollection<String> nodeTsv = conceptIdToCounts.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String conceptId = c.element().getKey();
				Long count = c.element().getValue();
				Map<String, String> conceptLabelMap = c.sideInput(conceptIdToLabelMap);

				String label = conceptLabelMap.get(conceptId);

				if (label == null) {
					label = "UKNOWN";
				}
				String line = String.format("%d\t%s\t%s", count, conceptId, label);

				c.output(line);
			}

		}).withSideInputs(conceptIdToLabelMap));
		return nodeTsv;
	}
}
