package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;

import edu.cuanschutz.ccp.tm_provider.etl.fn.EtlFailureToEntityFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ExtractedSentence;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SentenceExtractionFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.SentenceTsvBuilderFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

/**
 * Useful for extracting sentences for creating corpora for manual annotation
 */
public class SentenceExtractionPipeline {

	private static final PipelineKey PIPELINE_KEY = PipelineKey.SENTENCE_EXTRACTION;

//	private static final Logger logger = Logger.getLogger(SentenceExtractionPipeline.class);

	public interface Options extends DataflowPipelineOptions {
		@Description("The targetProcessingStatusFlag should align with the concept type served by the OGER service URI; pipe-delimited list")
		@Required
		ProcessingStatusFlag getTargetProcessingStatusFlag();

		void setTargetProcessingStatusFlag(ProcessingStatusFlag flag);

		@Description("Defines the documents required for input in order to extract the sentences appropriately. The string is a semi-colon "
				+ "delimited between different document criteria and pipe-delimited within document criteria, "
				+ "e.g.  TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0")
		@Required
		String getInputDocumentCriteria();

		void setInputDocumentCriteria(String docCriteria);

		@Description("Keywords to include in the sentences that are extracted. Input is a pipe-delimited list of keywords")
		@Required
		String getKeywords();

		void setKeywords(String keywords);

		@Description("prefix of the concept type, e.g. CHEBI, CL, etc. Must align with placeholder X. Can be a pipe-delimited list of multiple prefixes.")
		@Required
		String getPrefixX();

		void setPrefixX(String prefix);

		@Description("placeholder of the concept type, e.g. CHEBI, CL, etc. Must align with prefix X.")
		@Required
		String getPlaceholderX();

		void setPlaceholderX(String placeholder);

		@Description("prefix of the concept type, e.g. CHEBI, CL, etc.  Must align with placeholder Y. Can be a pipe-delimited list of multiple prefixes.")
		@Required
		String getPrefixY();

		void setPrefixY(String prefix);

		@Description("placeholder of the concept type, e.g. CHEBI, CL, etc. Must align with prefix Y.")
		@Required
		String getPlaceholderY();

		void setPlaceholderY(String placeholder);

//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentType getTargetDocumentType();
//
//		void setTargetDocumentType(DocumentType type);
//
//		@Description("The targetDocumentType should also align with the concept type served by the OGER service URI; pipe-delimited list")
//		DocumentFormat getTargetDocumentFormat();
//
//		void setTargetDocumentFormat(DocumentFormat type);
//
//		@Description("This pipeline key will be used to select the input text documents that will be processed")
//		PipelineKey getInputPipelineKey();
//
//		void setInputPipelineKey(PipelineKey value);
//
//		@Description("This pipeline version will be used to select the input text documents that will be processed")
//		String getInputPipelineVersion();
//
//		void setInputPipelineVersion(String value);

		@Description("The document collection to process")
		@Required
		String getCollection();

		void setCollection(String value);

		@Description("Path to the bucket where results will be written")
		@Required
		String getOutputBucket();

		void setOutputBucket(String bucketPath);

		@Description("Overwrite any previous runs")
		@Required
		OverwriteOutput getOverwrite();

		void setOverwrite(OverwriteOutput value);

		@Description("path to (pattern for) the file(s) containing mappings from ontology class to ancestor classes")
		@Required
		String getAncestorMapFilePath();

		void setAncestorMapFilePath(String path);

		@Description("delimiter used to separate columns in the ancestor map file")
		@Required
		Delimiter getAncestorMapFileDelimiter();

		void setAncestorMapFileDelimiter(Delimiter delimiter);

		@Description("delimiter used to separate items in the set in the second column of the ancestor map file")
		@Required
		Delimiter getAncestorMapFileSetDelimiter();

		void setAncestorMapFileSetDelimiter(Delimiter delimiter);

		@Description("CURIEs indicating concept identifiers that should be excluded from the extracted sentences")
		@Required
		String getConceptIdsToExclude();

		void setConceptIdsToExclude(String path);

		@Description("An optional collection that can be used when retrieving documents that do not below to the same collection as the status entity. This is helpful when only the status entity has been assigned to a particular collection that we want to process, e.g., the redo collections.")
		String getOptionalDocumentSpecificCollection();

		void setOptionalDocumentSpecificCollection(String value);

	}

	public static void main(String[] args) {
		String pipelineVersion = Version.getProjectVersion();
		com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Set<String> conceptIdsToExclude = new HashSet<String>(
				Arrays.asList(options.getConceptIdsToExclude().split("\\|")));

//		logger.info(" ---------------- Pipeline arguments for the SentenceExtractionPipeline ---------------- ");
//		logger.info("Collection: " + options.getCollection());
//		logger.info("Keywords: " + options.getKeywords().toString());
//		logger.info("Placeholder X: " + options.getPlaceholderX());
//		logger.info("Placeholder Y: " + options.getPlaceholderY());
//		logger.info("Prefix X: " + options.getPrefixX());
//		logger.info("Prefix Y: " + options.getPrefixY());
//		logger.info("Input Doc Criteria: " + options.getInputDocumentCriteria().toString());
//		logger.info("Output bucket: " + options.getOutputBucket());

		ProcessingStatusFlag targetProcessingStatusFlag = options.getTargetProcessingStatusFlag();
		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		Set<DocumentCriteria> inputDocCriteria = PipelineMain
				.compileInputDocumentCriteria(options.getInputDocumentCriteria());
		Set<String> keywords = compileKeywords(options.getKeywords());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> statusEntity2Content = PipelineMain
				.getStatusEntity2Content(inputDocCriteria, options.getProject(), p, targetProcessingStatusFlag,
						requiredProcessStatusFlags, options.getCollection(), options.getOverwrite(),
						options.getOptionalDocumentSpecificCollection());

		DocumentCriteria outputDocCriteria = new DocumentCriteria(DocumentType.CONCEPT_CHEBI, DocumentFormat.BIONLP,
				PIPELINE_KEY, pipelineVersion);

		// the extracted sentence output contains a version of the sentence where the
		// concepts have been replaced by placeholders. This map determines which
		// concept type is replaced by which placeholder.
		Map<List<String>, String> prefixesToPlaceholderMap = buildPrefixToPlaceholderMap(options.getPrefixX(),
				options.getPlaceholderX(), options.getPrefixY(), options.getPlaceholderY());
//		for (String xPrefix : options.getPrefixX().split("\\|")) {
//			prefixToPlaceholderMap.put(xPrefix, options.getPlaceholderX());
//		}
//		for (String yPrefix : options.getPrefixY().split("\\|")) {
//			prefixToPlaceholderMap.put(yPrefix, options.getPlaceholderY());
//		}

		DocumentType conceptDocumentType = extractConceptDocumentTypeFromInputDocCriteria(inputDocCriteria);

		final PCollectionView<Map<String, Set<String>>> ancestorMapView = PCollectionUtil.fromKeyToSetTwoColumnFiles(
				"ancestor map", p, options.getAncestorMapFilePath(), options.getAncestorMapFileDelimiter(),
				options.getAncestorMapFileSetDelimiter(), Compression.GZIP).apply(View.<String, Set<String>>asMap());

		PCollectionTuple output = SentenceExtractionFn.process(statusEntity2Content, keywords, outputDocCriteria,
				timestamp, inputDocCriteria, prefixesToPlaceholderMap, conceptDocumentType, ancestorMapView,
				conceptIdsToExclude);

		PCollection<KV<ProcessingStatus, ExtractedSentence>> extractedSentences = output
				.get(SentenceExtractionFn.EXTRACTED_SENTENCES_TAG);
		PCollection<EtlFailureData> failures = output.get(SentenceExtractionFn.ETL_FAILURE_TAG);

		/*
		 * store failures from sentence extraction
		 */
		PCollection<KV<String, Entity>> failureEntities = failures.apply("ext failures->datastore",
				ParDo.of(new EtlFailureToEntityFn()));
		PCollection<Entity> nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		// de-duplication of extracted sentences?
		output = SentenceTsvBuilderFn.process(extractedSentences, outputDocCriteria, timestamp);

		PCollection<KV<ProcessingStatus, String>> statusToOutputTsv = output.get(SentenceTsvBuilderFn.OUTPUT_TSV_TAG);
		failures = output.get(SentenceTsvBuilderFn.ETL_FAILURE_TAG);

		/*
		 * store failures from output file format creation
		 */
		failureEntities = failures.apply("tsv failures->datastore", ParDo.of(new EtlFailureToEntityFn()));
		nonredundantFailureEntities = PipelineMain.deduplicateEntitiesByKey(failureEntities);
		nonredundantFailureEntities.apply("failure_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		/*
		 * update the status entities to reflect the work completed, and store in
		 * Datastore while ensuring no duplicates are sent to Datastore for storage.
		 * 
		 * We may run this pipeline manually to extract sentences to be part of training
		 * sets. When doing so we'll use the NOOP flag so that the status is not flagged
		 * as having its sentences extracted for classification.
		 */
		if (targetProcessingStatusFlag != ProcessingStatusFlag.NOOP) {
			PCollection<Entity> updatedEntities = PipelineMain.updateStatusEntities(
					statusToOutputTsv.apply(Keys.<ProcessingStatus>create()), targetProcessingStatusFlag);
			PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
			nonredundantStatusEntities.apply("status_entity->datastore",
					DatastoreIO.v1().write().withProjectId(options.getProject()));
		}

		// output sentences to file
		PCollection<String> outputTsv = statusToOutputTsv
				.apply(ParDo.of(new DoFn<KV<ProcessingStatus, String>, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<ProcessingStatus, String> element = c.element();
						c.output(element.getValue());
					}
				}));
		outputTsv.apply("write tsv",
				TextIO.write().to(options.getOutputBucket()).withSuffix("." + options.getCollection() + ".tsv"));

		p.run().waitUntilFinish();
	}

	protected static Map<List<String>, String> buildPrefixToPlaceholderMap(String prefixesX, String placeholderX,
			String prefixesY, String placeholderY) {
		Map<List<String>, String> prefixesToPlaceholderMap = new HashMap<List<String>, String>();
		List<String> xPrefixes = Arrays.asList(prefixesX.split("\\|"));
		Collections.sort(xPrefixes);

		List<String> yPrefixes = Arrays.asList(prefixesY.split("\\|"));
		Collections.sort(yPrefixes);

		prefixesToPlaceholderMap.put(xPrefixes, placeholderX);
		if (!prefixesToPlaceholderMap.containsKey(yPrefixes)) {
			prefixesToPlaceholderMap.put(yPrefixes, placeholderY);
		}
		return prefixesToPlaceholderMap;
	}

	private static DocumentType extractConceptDocumentTypeFromInputDocCriteria(Set<DocumentCriteria> inputDocCriteria) {
		for (DocumentCriteria dc : inputDocCriteria) {
			if (dc.getDocumentType() == DocumentType.CONCEPT_ALL) {
				return DocumentType.CONCEPT_ALL;
			} else if (dc.getDocumentType() == DocumentType.CONCEPT_ALL_UNFILTERED) {
				return DocumentType.CONCEPT_ALL_UNFILTERED;
			}
		}
		throw new IllegalArgumentException(
				"No concept document type was specified in the input document criteria. Processing cannot continue.");
	}

	@VisibleForTesting
	protected static Set<String> compileKeywords(String keywords) {
		if (keywords == null || keywords.isEmpty()) {
			return new HashSet<String>();
		}
		return new HashSet<String>(Arrays.asList(keywords.split("\\|")));
	}

}
