package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;

/**
 * This pipeline adds identifiers to a file-based collection of identifiers
 */
public class UpdateIdCollectionPipeline {

	private static final String COLLECTION_FILE_SUFFIX = ".ids";
	private static final Compression COLLECTION_COMPRESSION = Compression.GZIP;
	private static final Compression INPUT_COMPRESSION = Compression.UNCOMPRESSED;

	public interface Options extends DataflowPipelineOptions {
		@Description("GCP file prefix for files that make up the identifier collection")
		@Required
		String getCollectionFilePrefix();

		void setCollectionFilePrefix(String value);

		@Description("GCP file pattern for the file(s) containing potential identifiers to add")
		@Required
		String getInputFilePattern();

		void setInputFilePattern(String value);

		@Description("date stamp is used as part of the output file names to ensure the output file names don't get overwritten each day")
		@Required
		String getDateStamp();

		void setDateStamp(String value);
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		/* load the current collection of identifiers into a PCollectionView */
		String collectionFilePattern = String.format("%s*", options.getCollectionFilePrefix());
		final PCollectionView<Set<String>> idCollectionView = PCollectionUtil.loadSetAsMapFromFile("load-collection", p,
				collectionFilePattern, COLLECTION_COMPRESSION);

		/* load the input collection of identifiers into another PCollectionView */
		PCollection<String> newIds = p.apply("load-input-ids",
				TextIO.read().from(options.getInputFilePattern()).withCompression(INPUT_COMPRESSION));

		/*
		 * get a collection of new IDs that are not currently members of the ID
		 * collection
		 */
		PCollection<String> idsToAddToCollection = SetDiffFn.process(idCollectionView, newIds);

		/* write to file any new identifiers not in the existing collection */
		idsToAddToCollection.apply(Distinct.<String>create()).apply("write-new-ids",
				TextIO.write().to(options.getCollectionFilePrefix() + "." + options.getDateStamp())
						.withSuffix(COLLECTION_FILE_SUFFIX).withCompression(COLLECTION_COMPRESSION));

		p.run().waitUntilFinish();

	}

	/**
	 * This DoFn takes as input a PCollection of potentially new identifiers and a
	 * PCollectionView of the current identifier collection. It outputs a
	 * PCollection of identifiers that are not in the current collection.
	 *
	 */
	public static class SetDiffFn extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;

		public static PCollection<String> process(PCollectionView<Set<String>> idCollectionView,
				PCollection<String> newIds) {
			return newIds.apply("extract-new-ids", ParDo.of(new DoFn<String, String>() {
				private static final long serialVersionUID = 1L;

				@ProcessElement
				public void processElement(ProcessContext context) {
					String id = context.element();
					Set<String> existingIds = context.sideInput(idCollectionView);
					if (!existingIds.contains(id)) {
						context.output(id);
					}
				}
			}).withSideInputs(idCollectionView));
		}

	}

}
