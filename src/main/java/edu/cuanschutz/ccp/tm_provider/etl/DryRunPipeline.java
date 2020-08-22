package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * This Apache Beam pipeline is useful for testing pipeline input arguments. It
 * performs a "dry run" in that it simply lists the document IDs that will be
 * processed with the input arguments as specified.
 */
public class DryRunPipeline {

	private final static Logger LOGGER = Logger.getLogger(DryRunPipeline.class.getName());

	public interface Options extends DataflowPipelineOptions {
		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
		DocumentType getTargetDocumentType();

		void setTargetDocumentType(DocumentType type);

//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentFormat getTargetDocumentFormat();
//
//		void setTargetDocumentFormat(DocumentFormat type);
//
		@Description("This pipeline key will be used to select the input text documents that will be processed")
		PipelineKey getInputPipelineKey();

		void setInputPipelineKey(PipelineKey value);

		@Description("This pipeline version will be used to select the input text documents that will be processed")
		String getInputPipelineVersion();

		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("Name of the directory where the IDs that will be processed are written to file(s)")
		String getOutputDirectory();

		void setOutputDirectory(String value);

		@Description("Overwrite any previous runs")
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		LOGGER.log(Level.INFO, String.format("Running OGER pipeline for concept: ", options.getTargetDocumentType()));

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		DocumentType targetDocumentType = options.getTargetDocumentType();
		LOGGER.log(Level.INFO, String.format("Initializing pipeline for: %s -- %s -- collection: %s",
				targetProcessingStatusFlag.name(), targetDocumentType.name(), options.getCollection()));

		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		DocumentCriteria inputTextDocCriteria = new DocumentCriteria(DocumentType.TEXT, DocumentFormat.TEXT,
				options.getInputPipelineKey(), options.getInputPipelineVersion());
		PCollection<KV<Entity, Map<DocumentCriteria, String>>> docId2Content = PipelineMain.getStatusEntity2Content(
				Arrays.asList(inputTextDocCriteria), options.getProject(), p, targetProcessingStatusFlag,
				requiredProcessStatusFlags, options.getCollection(), options.getOverwrite());

		docId2Content.apply(Keys.<Entity>create()).apply("extract-doc-id", ParDo.of(new DoFn<Entity, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				Entity statusEntity = c.element();
				String documentId = DatastoreProcessingStatusUtil.getDocumentId(statusEntity);
				// if there are more than one entity, we just return one
				c.output(documentId);
			}
		})).apply("write ids file",
				TextIO.write().to(options.getOutputDirectory() + "/annotation.").withSuffix(".txt"));

		p.run().waitUntilFinish();
	}

}
