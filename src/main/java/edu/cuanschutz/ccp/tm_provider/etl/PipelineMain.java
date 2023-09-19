package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_FORMAT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;

import edu.cuanschutz.ccp.tm_provider.etl.deprecated.BigQueryExportPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.deprecated.WebAnnoSentenceExtractionPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.update.UpdateMedlineEntitiesPipeline;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class PipelineMain {

	private final static Logger LOGGER = Logger.getLogger(PipelineMain.class.getName());
	public static final String PIPELINE_VERSION_RECENT = "recent";

	private static final TupleTag<ProcessingStatus> statusTag = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag1 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag2 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag3 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag4 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag5 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag6 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag7 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag8 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag9 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag10 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag11 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag12 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag13 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag14 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag15 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag16 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag17 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag18 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag19 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag20 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag21 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag22 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag23 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag24 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag25 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag26 = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag27 = new TupleTag<>();

	public enum MultithreadedServiceCalls {
		ENABLED, DISABLED
	}

	public static void main(String[] args) {
		System.out.println("Running pipeline version: " + Version.getProjectVersion());

		PipelineKey pipeline = null;
		try {
			pipeline = PipelineKey.valueOf(args[0]);
		} catch (IllegalArgumentException e) {
			System.err.println(String.format("Unrecognized pipeline (%s). Valid choices are %s", args[0],
					Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			pipeline = null;
		}
		if (pipeline != null) {
			String[] pipelineArgs = Arrays.copyOfRange(args, 1, args.length);
			switch (pipeline) {
			case ABBREVIATION:
				AbbreviationAb3pPipeline.main(pipelineArgs);
				break;
			case BIOC_TO_TEXT:
				BiocToTextPipeline.main(pipelineArgs);
				break;
			case COLLECTION_ASSIGNMENT:
				CollectionAssignmentPipeline.main(pipelineArgs);
				break;
			case CRF:
				CrfNerPipeline.main(pipelineArgs);
				break;
			case CONCEPT_COUNT_DISTRIBUTION:
				ConceptCountDistributionPipeline.main(pipelineArgs);
				break;
			case CONCEPT_POST_PROCESS:
				ConceptPostProcessingPipeline.main(pipelineArgs);
				break;
			case OGER_POST_PROCESS:
				OgerPostProcessingPipeline.main(pipelineArgs);
				break;
			case CONCEPT_ANNOTATION_EXPORT:
				ConceptAnnotationExportPipeline.main(pipelineArgs);
				break;
			case MEDLINE_XML_TO_TEXT:
				MedlineXmlToTextPipeline.main(pipelineArgs);
				break;
			case CONCEPT_COOCCURRENCE_COUNTS:
				ConceptCooccurrenceCountsPipeline.main(pipelineArgs);
				break;
			case CONCEPT_COOCCURRENCE_METRICS:
				ConceptCooccurrenceMetricsPipeline.main(pipelineArgs);
				break;
			case CONCEPT_IDF:
				ConceptIdfPipeline.main(pipelineArgs);
				break;
			// This pipeline is not actively used.
//			case DEPENDENCY_PARSE:
//				DependencyParsePipeline.main(pipelineArgs);
//				break;
			case FILE_LOAD:
				LoadFilesPipeline.main(pipelineArgs);
				break;
			case OGER:
				OgerPipeline.main(pipelineArgs);
				break;
			case BIGQUERY_EXPORT:
				BigQueryExportPipeline.main(pipelineArgs);
				break;
			case SENTENCE_EXTRACTION:
				SentenceExtractionPipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE_IMPORT:
				DependencyParseStoragePipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE_TO_SENTENCE:
				DependencyParseToSentencePipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE_TO_CONLL03:
				DependencyParseToConll03Pipeline.main(pipelineArgs);
				break;
			case SENTENCE_SEGMENTATION:
				SentenceSegmentationPipeline.main(pipelineArgs);
				break;
			case SENTENCE_COOCCURRENCE_EXPORT:
				SentenceCooccurrencePipeline.main(pipelineArgs);
				break;
			case WEBANNO_SENTENCE_EXTRACTION:
				WebAnnoSentenceExtractionPipeline.main(pipelineArgs);
				break;
			case CLASSIFIED_SENTENCE_STORAGE:
				ClassifiedSentenceStoragePipeline.main(pipelineArgs);
				break;
			case UPDATE_MEDLINE_STATUS_ENTITIES:
				UpdateMedlineEntitiesPipeline.main(pipelineArgs);
				break;
			case ELASTICSEARCH_LOAD:
				ElasticsearchLoadPipeline.main(pipelineArgs);
				break;
			case DOC_TEXT_AUGMENTATION:
				DocumentTextAugmentationPipeline.main(pipelineArgs);
				break;
			case FILTER_UNACTIONABLE_TEXT:
				FilterUnactionableTextPipeline.main(pipelineArgs);
				break;
			case TEXT_EXPORT:
				TextExtractionPipeline.main(pipelineArgs);
				break;
			case DRY_RUN:
				DryRunPipeline.main(pipelineArgs);
				break;
			default:
				throw new IllegalArgumentException(String.format(
						"Valid pipeline (%s) but a code change required before it can be used. Valid choices are %s",
						args[0],
						Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			}
		}
	}

	private static Map<Integer, TupleTag<ProcessedDocument>> populateTagMap() {
		Map<Integer, TupleTag<ProcessedDocument>> tagMap = new HashMap<Integer, TupleTag<ProcessedDocument>>();

		tagMap.put(1, documentTag1);
		tagMap.put(2, documentTag2);
		tagMap.put(3, documentTag3);
		tagMap.put(4, documentTag4);
		tagMap.put(5, documentTag5);
		tagMap.put(6, documentTag6);
		tagMap.put(7, documentTag7);
		tagMap.put(8, documentTag8);
		tagMap.put(9, documentTag9);
		tagMap.put(10, documentTag10);
		tagMap.put(11, documentTag11);
		tagMap.put(12, documentTag12);
		tagMap.put(13, documentTag13);
		tagMap.put(14, documentTag14);
		tagMap.put(15, documentTag15);
		tagMap.put(16, documentTag16);
		tagMap.put(17, documentTag17);
		tagMap.put(18, documentTag18);
		tagMap.put(19, documentTag19);
		tagMap.put(20, documentTag20);
		tagMap.put(21, documentTag21);
		tagMap.put(22, documentTag22);
		tagMap.put(23, documentTag23);
		tagMap.put(24, documentTag24);
		tagMap.put(25, documentTag25);
		tagMap.put(26, documentTag26);
		tagMap.put(27, documentTag27);

		return tagMap;
	}

	/**
	 * @param inputDocCriteria
	 * @param gcpProjectId
	 * @param beamPipeline
	 * @param targetProcessStatusFlag
	 * @param requiredProcessStatusFlags
	 * @param collection
	 * @param overwriteOutput
	 * @param constrainDocumentsToCollection if YES, then the collection is added to
	 *                                       the filters when searching for
	 *                                       documents; if NO, the collection filter
	 *                                       is excludeds
	 * @return a mapping from the status entity to various document content (a
	 *         mapping from the document criteria to the document content)
	 */
	public static PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> getStatusEntity2Content(
			Set<DocumentCriteria> inputDocCriteria, String gcpProjectId, Pipeline beamPipeline,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String collection, OverwriteOutput overwriteOutput, String documentSpecificCollection) {

		Map<Integer, TupleTag<ProcessedDocument>> tagMap = populateTagMap();

		/*
		 * get the status entities for documents that meet the required process status
		 * flag critera but whose target process status flag is false
		 */
		PCollection<KV<String, ProcessingStatus>> docId2Status = getStatusEntitiesToProcess(beamPipeline,
				targetProcessStatusFlag, requiredProcessStatusFlags, gcpProjectId, collection, overwriteOutput);

		KeyedPCollectionTuple<String> tuple = KeyedPCollectionTuple.of(statusTag, docId2Status);

		// current code allows up to 25 document criteria to be added. Code can be
		// extended if more are needed. It doesn't seem possible to do this (create
		// tuple tags) dynamically.

		if (inputDocCriteria.size() > tagMap.size()) {
			throw new IllegalArgumentException(String.format(
					"Cannot have >%d input document criteria. Code can be extended, but code revision is required.",
					tagMap.size()));
		}

		int tagIndex = 1;
		Map<Integer, DocumentCriteria> tagIndexToDocCriteriaMap = new HashMap<Integer, DocumentCriteria>();
		for (DocumentCriteria docCriteria : inputDocCriteria) {
			String documentCollection = collection;
			if (documentSpecificCollection != null && !documentSpecificCollection.trim().isEmpty()
					&& !documentSpecificCollection.equalsIgnoreCase("null")) {
				documentCollection = documentSpecificCollection;
			}

			PCollection<KV<String, ProcessedDocument>> docId2Document = getDocumentEntitiesToProcess(beamPipeline,
					docCriteria, documentCollection, gcpProjectId);
			tagIndexToDocCriteriaMap.put(tagIndex, docCriteria);
			tuple = tuple.and(tagMap.get(tagIndex++), docId2Document);
		}

		PCollection<KV<String, CoGbkResult>> result = tuple.apply("merge status entities with docs",
				CoGroupByKey.create());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> outputPCollection = result.apply(
				"check for required docs",
				ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<ProcessingStatus, Map<DocumentCriteria, String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> element = c.element();
						CoGbkResult result = element.getValue();

						try {
							// when doing the redo runs, it is possible that there are documents collected
							// (b/c we are unable to be specific about which collection to use for the
							// documents) but no processing status entity. When we were sure that a
							// processing status would be returned, we used to use
							// result.getOnly(statusTag), however, now we must use result.getAll(statusTag)
							// and then check to see if the result is null or empty before proceeding.
//							ProcessingStatus processingStatus = result.getOnly(statusTag);
							Iterable<ProcessingStatus> allProcessingStatus = result.getAll(statusTag);
							// there will be a max of one in this iterable
							if (allProcessingStatus != null) {
								for (ProcessingStatus processingStatus : allProcessingStatus) {
									if (processingStatus != null) {
										// get all associated documents -- there should be only one
										// ProcessedDocument associated with each documentTag
										Set<ProcessedDocument> allDocuments = new HashSet<ProcessedDocument>();
										boolean hasAllDocuments = true;
										DocumentCriteria missingDocCriteria = null;
										// index starts with 1 here b/c the tagMap keys start with 1
										for (int index = 1; index <= inputDocCriteria.size(); index++) {
											// we use result.getAll here b/c it is possible for multiple documents to be
											// returned for a single DocumentCriteria if the pipeline version is set to
											// "recent" instead of a specific semantic version, e.g. 0.1.2.
											Iterable<ProcessedDocument> allDocs = result.getAll(tagMap.get(index));
											if (allDocs != null) {
												for (ProcessedDocument doc : result.getAll(tagMap.get(index))) {
													allDocuments.add(doc);
												}
											} else {
												missingDocCriteria = tagIndexToDocCriteriaMap.get(index);
												LOGGER.log(Level.WARNING, String.format(
														"Skipping processing due to missing documents for id: %s criteria: %s",
														element.getKey(), missingDocCriteria.toString()));
												hasAllDocuments = false;
												break;
											}

										}

										if (hasAllDocuments) {
											// piece together documents that have been split for storage
											Map<DocumentCriteria, String> contentMap = spliceDocumentChunks(
													allDocuments);

											// if there are documents with the same pipeline key, type, and format, but
											// with
											// different versions, then keep the most recent (highest) version
											Map<DocumentCriteria, String> filteredContentMap = filterForMostRecent(
													contentMap);

											c.output(KV.of(processingStatus, filteredContentMap));
										}
									}
								}
							}
						} catch (IllegalArgumentException e) {
							LOGGER.log(Level.WARNING,
									"Skipping processing due to IllegalArgumentException for id: " + element.getKey(),
									e);
						}
					}

				}));

		return outputPCollection;
	}

	@VisibleForTesting
	protected static Map<DocumentCriteria, String> filterForMostRecent(Map<DocumentCriteria, String> contentMap) {
		Map<String, Map<DocumentCriteria, String>> map = new HashMap<String, Map<DocumentCriteria, String>>();

		// organize the documents by a key that does not include the pipeline version,
		// but does include pipeline key, doc type, doc format
		for (Entry<DocumentCriteria, String> entry : contentMap.entrySet()) {
			String key = createVersionAgnosticKey(entry.getKey());
			if (map.containsKey(key)) {
				map.get(key).put(entry.getKey(), entry.getValue());
			} else {
				Map<DocumentCriteria, String> innerMap = new HashMap<DocumentCriteria, String>();
				innerMap.put(entry.getKey(), entry.getValue());
				map.put(key, innerMap);
			}
		}

		// now keep only the most recent version of a given document
		// NOTE: if we ever want to import multiple versions of the same document - this
		// will prevent such an operation
		Map<DocumentCriteria, String> filteredContentMap = new HashMap<DocumentCriteria, String>();
		for (Entry<String, Map<DocumentCriteria, String>> entry : map.entrySet()) {
			KV<DocumentCriteria, String> mostRecent = getMostRecent(entry.getValue());
			filteredContentMap.put(mostRecent.getKey(), mostRecent.getValue());
		}

		return filteredContentMap;

	}

	/**
	 * Given a mapping from DocumentCriteria to document content, return the
	 * <DocumentCriteria,content> pair that is most recent, i.e., has the highest
	 * semantic version
	 * 
	 * @param map
	 * @return
	 */
	@VisibleForTesting
	protected static KV<DocumentCriteria, String> getMostRecent(Map<DocumentCriteria, String> map) {
		String previousVersion = null;
		KV<DocumentCriteria, String> mostRecent = null;
		for (Entry<DocumentCriteria, String> entry : map.entrySet()) {
			DocumentCriteria dc = entry.getKey();
			if (previousVersion == null || isMoreRecent(dc.getPipelineVersion(), previousVersion)) {
				mostRecent = KV.of(dc, entry.getValue());
				previousVersion = dc.getPipelineVersion();
			}
		}

		return mostRecent;
	}

	/**
	 * @param version1
	 * @param version2
	 * @return true is version1 is more recent than version2
	 */
	@VisibleForTesting
	protected static boolean isMoreRecent(String version1, String version2) {
		int[] semanticVersion1 = getSemanticVersion(version1);
		int[] semanticVersion2 = getSemanticVersion(version2);

		// we assume that at a maximum the version has 3 numbers, e.g. 0.4.7
		for (int i = 0; i < 3; i++) {
			// starting with the left-most number we compare versions. If at any point
			// version1 is > version2 then return true. If version1 is < version2 then
			// return false. If version1==version2, then the loop will move on to the next
			// number. If the versions happen to be identical, which should not happen, then
			// false is returned.
			if (semanticVersion1[i] > semanticVersion2[i]) {
				return true;
			} else if (semanticVersion1[i] < semanticVersion2[i]) {
				return false;
			}
		}

		return false;
	}

	@VisibleForTesting
	protected static int[] getSemanticVersion(String version) {
		String[] cols = version.split("\\.");
		if (cols.length > 3) {
			throw new IllegalArgumentException("Unable to handle semantic versions with > 3 numbers: " + version);
		}
		int[] semanticVersion = new int[3];
		for (int i = 0; i < 3; i++) {
			if (cols.length > i) {
				semanticVersion[i] = Integer.parseInt(cols[i]);
			} else {
				// if a version has fewer than three numbers, then use zero as a placeholder,
				// e.g., 1.2 == 1.2.0
				semanticVersion[i] = 0;
			}
		}
		return semanticVersion;
	}

	/**
	 * Creates a key that does not include the pipeline version, but does include
	 * pipeline key, doc type, doc format
	 * 
	 * @param dc
	 * @return
	 */
	public static String createVersionAgnosticKey(DocumentCriteria dc) {
		return String.format("%s_%s_%s", dc.getPipelineKey().name(), dc.getDocumentType().name(),
				dc.getDocumentFormat().name());
	}

	@VisibleForTesting
	protected static Map<DocumentCriteria, String> spliceDocumentChunks(Iterable<ProcessedDocument> documents) {
		Map<DocumentCriteria, Map<Long, String>> map = new HashMap<DocumentCriteria, Map<Long, String>>();

		// map content by chunk id so that it can be sorted and spliced back together
		for (ProcessedDocument document : documents) {
			DocumentCriteria docCriteria = document.getDocumentCriteria();
			long chunkId = document.getChunkId();
			String content = document.getDocumentContent();

			if (map.containsKey(docCriteria)) {
				map.get(docCriteria).put(chunkId, content);
			} else {
				Map<Long, String> innerMap = new HashMap<Long, String>();
				innerMap.put(chunkId, content);
				map.put(docCriteria, innerMap);
			}
		}

		// splice the documents together
		Map<DocumentCriteria, String> outputMap = new HashMap<DocumentCriteria, String>();
		for (Entry<DocumentCriteria, Map<Long, String>> entry : map.entrySet()) {
			DocumentCriteria docCriteria = entry.getKey();
			Map<Long, String> chunkMap = entry.getValue();
			Map<Long, String> sortedChunkMap = CollectionsUtil.sortMapByKeys(chunkMap, SortOrder.ASCENDING);
			StringBuilder sb = new StringBuilder();
			for (Entry<Long, String> chunkEntry : sortedChunkMap.entrySet()) {
				sb.append(chunkEntry.getValue());
			}
			outputMap.put(docCriteria, sb.toString());
		}

		return outputMap;

	}

	public static PCollection<KV<String, ProcessingStatus>> getStatusEntitiesToProcess(Pipeline p,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String gcpProjectId, String collection, OverwriteOutput overwriteOutput) {
		LOGGER.log(Level.INFO, String.format("PROCESSING STATUS FILTER SETTINGS: \nOVERWRITE: %s\nCOLLECTION: %s",
				overwriteOutput.name(), collection));
		List<Filter> filters = new ArrayList<Filter>();
		for (ProcessingStatusFlag flag : requiredProcessStatusFlags) {
			Filter filter = makeFilter(flag.getDatastoreFlagPropertyName(), PropertyFilter.Operator.EQUAL,
					makeValue(true)).build();
			filters.add(filter);
		}

		/*
		 * targetProcessStatusFlag is null when we won't be updating the status document
		 * on the results of this processing run, e.g. when we are not adding data to
		 * Datastore but are simply exporting content to a bucket, e.g. the
		 * ConceptAnnotationExportPipeline
		 */
		if (overwriteOutput == OverwriteOutput.NO && targetProcessStatusFlag != null
				&& targetProcessStatusFlag != ProcessingStatusFlag.NOOP) {
			filters.add(makeFilter(targetProcessStatusFlag.getDatastoreFlagPropertyName(),
					PropertyFilter.Operator.EQUAL, makeValue(false)).build());
		}

		/* incorporate the collection name into the query if there is one */
		if (collection != null) {
			filters.add(makeFilter(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS, PropertyFilter.Operator.EQUAL,
					makeValue(collection)).build());
		}

		for (Filter filter : filters) {
			LOGGER.log(Level.INFO, "PROCESSING STATUS FILTER: " + filter.toString());
		}

		Filter filter = makeAndFilter(filters).build();
		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(STATUS_KIND);
		query.setFilter(filter);

		PCollection<Entity> status = p.apply(String.format("load status entities - %s", collection),
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(gcpProjectId));

		PCollection<KV<String, ProcessingStatus>> docId2Status = status.apply("status entity->status",
				ParDo.of(new DoFn<Entity, KV<String, ProcessingStatus>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element Entity statusEntity,
							OutputReceiver<KV<String, ProcessingStatus>> out) {
						ProcessingStatus ps = new ProcessingStatus(statusEntity);
						out.output(KV.of(ps.getDocumentId(), ps));
					}
				}));

		return docId2Status;
	}

	/**
	 * @param p
	 * @param docCriteria
	 * @param collection
	 * @param gcpProjectId
	 * @param documentSpecificCollection if assigned, this collection will be used
	 *                                   in the search for documents
	 * @return
	 */
	public static PCollection<KV<String, ProcessedDocument>> getDocumentEntitiesToProcess(Pipeline p,
			DocumentCriteria docCriteria, String collection, String gcpProjectId) {
		DocumentFormat documentFormat = docCriteria.getDocumentFormat();
		DocumentType documentType = docCriteria.getDocumentType();
		PipelineKey pipelineKey = docCriteria.getPipelineKey();
		String pipelineVersion = docCriteria.getPipelineVersion();

		List<Filter> filters = setFilters(collection, documentFormat, documentType, pipelineKey, pipelineVersion);

		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(DOCUMENT_KIND);

		if (!filters.isEmpty()) {
			Filter filter = makeAndFilter(filters).build();
			query.setFilter(filter);
		}

		PCollection<Entity> documents = p.apply(String.format("load %s - %s",
				(documentType == null) ? "all types" : documentType.name().toLowerCase(), collection),
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(gcpProjectId));

		PCollection<KV<String, ProcessedDocument>> docId2Document = documents.apply("document entity -> PD",
				ParDo.of(new DoFn<Entity, KV<String, ProcessedDocument>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element Entity statusEntity,
							OutputReceiver<KV<String, ProcessedDocument>> out) {

						try {
							ProcessedDocument pd = new ProcessedDocument(statusEntity);
							out.output(KV.of(pd.getDocumentId(), pd));
						} catch (UnsupportedEncodingException e) {
							throw new IllegalStateException("Error while converting from entity to ProcessedDocument.",
									e);
						}

					}
				}));

//		// group the documents by their document id
//		PCollection<KV<String, Iterable<ProcessedDocument>>> docId2Documents = docId2Document.apply("group-by-docid",
//				GroupByKey.<String, ProcessedDocument>create());

		return docId2Document;
	}

	/**
	 * Return the document IDs for documents that match the specified document
	 * criteria and collection
	 * 
	 * @param p
	 * @param docCriteria
	 * @param collection
	 * @param gcpProjectId
	 * @return
	 */
	public static PCollection<String> getDocumentIdsForExistingDocuments(Pipeline p, DocumentCriteria docCriteria,
			String collection, String gcpProjectId) {
		DocumentFormat documentFormat = docCriteria.getDocumentFormat();
		DocumentType documentType = docCriteria.getDocumentType();
		PipelineKey pipelineKey = docCriteria.getPipelineKey();
		String pipelineVersion = docCriteria.getPipelineVersion();

		List<Filter> filters = setFilters(collection, documentFormat, documentType, pipelineKey, pipelineVersion);

		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(DOCUMENT_KIND);

		if (!filters.isEmpty()) {
			Filter filter = makeAndFilter(filters).build();
			query.setFilter(filter);
		}

		PCollection<Entity> documents = p.apply(
				String.format("load %s", (documentType == null) ? "all types" : documentType.name().toLowerCase()),
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(gcpProjectId));

		PCollection<String> docId = documents.apply("extract doc id", ParDo.of(new DoFn<Entity, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(@Element Entity statusEntity, OutputReceiver<String> out) {

				try {
					ProcessedDocument pd = new ProcessedDocument(statusEntity);
					out.output(pd.getDocumentId());
				} catch (UnsupportedEncodingException e) {
					throw new IllegalStateException("Error while extracting document ID from entity.", e);
				}

			}
		}));

		return docId;
	}

	/**
	 * @param collection
	 * @param documentFormat
	 * @param documentType
	 * @param pipelineKey
	 * @param pipelineVersion
	 * @return
	 */
	private static List<Filter> setFilters(String collection, DocumentFormat documentFormat, DocumentType documentType,
			PipelineKey pipelineKey, String pipelineVersion) {
		List<Filter> filters = new ArrayList<Filter>();
		if (documentFormat != null) {
			Filter filter = makeFilter(DOCUMENT_PROPERTY_FORMAT, PropertyFilter.Operator.EQUAL,
					makeValue(documentFormat.name())).build();
			filters.add(filter);
		}

		if (documentType != null) {
			Filter filter = makeFilter(DOCUMENT_PROPERTY_TYPE, PropertyFilter.Operator.EQUAL,
					makeValue(documentType.name())).build();
			filters.add(filter);
		}

		if (pipelineKey != null) {
			Filter filter = makeFilter(DOCUMENT_PROPERTY_PIPELINE, PropertyFilter.Operator.EQUAL,
					makeValue(pipelineKey.name())).build();
			filters.add(filter);
		}

		// if the pipeline version is set to "recent" then we will retrieve all versions
		// of a given document and the most recent (highest semantic version number)
		// will be selected at a later time
		if (pipelineVersion != null && !pipelineVersion.equals("recent")) {
			Filter filter = makeFilter(DOCUMENT_PROPERTY_PIPELINE_VERSION, PropertyFilter.Operator.EQUAL,
					makeValue(pipelineVersion)).build();
			filters.add(filter);
		}

		/* incorporate the collection name into the query if there is one */
		if (collection != null) {
			filters.add(makeFilter(DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS, PropertyFilter.Operator.EQUAL,
					makeValue(collection)).build());
		}

		for (Filter filter : filters) {
			LOGGER.log(Level.INFO, "DOCUMENT FILTER: " + filter.toString());
		}
		return filters;
	}

	/**
	 * @param input
	 * @return the input string divided (if necessary) into chunks small enough to
	 *         fit under the max byte threshold imposed by DataStore
	 * @throws UnsupportedEncodingException
	 */
	public static List<String> chunkContent(String input) throws UnsupportedEncodingException {
		List<String> chunks = new ArrayList<String>();

		if (input.getBytes("UTF-8").length < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES) {
			chunks.add(input);
		} else {
			chunks.addAll(splitStringByByteLength(input, CharacterEncoding.UTF_8,
					DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES));
		}

		return chunks;
	}

	/**
	 * from:
	 * https://stackoverflow.com/questions/48868721/splitting-a-string-with-byte-length-limits-in-java
	 * 
	 * @param src
	 * @param encoding
	 * @param maxsize
	 * @return
	 */
	public static List<String> splitStringByByteLength(String src, CharacterEncoding encoding, int maxsize) {
		Charset cs = Charset.forName(encoding.getCharacterSetName());
		CharsetEncoder coder = cs.newEncoder();
		ByteBuffer out = ByteBuffer.allocate(maxsize); // output buffer of required size
		CharBuffer in = CharBuffer.wrap(src);
		List<String> ss = new ArrayList<>(); // a list to store the chunks
		int pos = 0;
		while (true) {
			CoderResult cr = coder.encode(in, out, true); // try to encode as much as possible
			int newpos = src.length() - in.length();
			String s = src.substring(pos, newpos);
			ss.add(s); // add what has been encoded to the list
			pos = newpos; // store new input position
			out.rewind(); // and rewind output buffer
			if (!cr.isOverflow()) {
				break; // everything has been encoded
			}
		}
		return ss;
	}

	public static PCollection<Entity> deduplicateStatusEntities(PCollection<Entity> statusEntities) {
		PCollection<KV<String, Entity>> documentIdToStatusEntity = statusEntities.apply("status-->doc_id/status",
				ParDo.of(new DoFn<Entity, KV<String, Entity>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Entity statusEntity = c.element();
						String documentId = DatastoreProcessingStatusUtil.getDocumentId(statusEntity);
						c.output(KV.of(documentId, statusEntity));
					}
				}));

		return deduplicateEntitiesByKey(documentIdToStatusEntity);
	}

	public static PCollection<Entity> deduplicateDocumentEntities(PCollection<Entity> documentEntities) {
		PCollection<KV<String, Entity>> documentIdToDocumentEntity = documentEntities.apply("document-->key/document",
				ParDo.of(new DoFn<Entity, KV<String, Entity>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Entity documentEntity = c.element();
						String documentId = documentEntity.getPropertiesMap().get(DOCUMENT_PROPERTY_ID)
								.getStringValue();
						String documentType = documentEntity.getPropertiesMap().get(DOCUMENT_PROPERTY_TYPE)
								.getStringValue();
						String documentFormat = documentEntity.getPropertiesMap().get(DOCUMENT_PROPERTY_FORMAT)
								.getStringValue();
						String pipelineKey = documentEntity.getPropertiesMap().get(DOCUMENT_PROPERTY_PIPELINE)
								.getStringValue();
						String pipelineVersion = documentEntity.getPropertiesMap()
								.get(DOCUMENT_PROPERTY_PIPELINE_VERSION).getStringValue();
						String compositeKey = String.format("%s-%s-%s-%s-%s", documentId, documentType, documentFormat,
								pipelineKey, pipelineVersion);
						c.output(KV.of(compositeKey, documentEntity));
					}
				}));

		return deduplicateEntitiesByKey(documentIdToDocumentEntity);
	}

	/**
	 * @param docId2Entity
	 * @return a non-redundant collection of the input entities filtered based on
	 *         the entity keys (the String in the KV pair)
	 */
	public static <T> PCollection<T> deduplicateEntitiesByKey(PCollection<KV<String, T>> docId2Entity) {
		// remove any duplicates
		PCollection<KV<String, Iterable<T>>> idToEntities = docId2Entity.apply("group-by-key",
				GroupByKey.<String, T>create());
		PCollection<T> nonredundantStatusEntities = idToEntities.apply("dedup-by-key",
				ParDo.of(new DoFn<KV<String, Iterable<T>>, T>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Iterable<T> entities = c.element().getValue();
						// if there are more than one entity, we just return one
						c.output(entities.iterator().next());
					}
				}));
		return nonredundantStatusEntities;
	}

	// TODO this method can replace the one above
	public static <T> PCollection<T> deduplicateByKey(PCollection<KV<String, T>> docId2Entity) {
		// remove any duplicates
		PCollection<KV<String, Iterable<T>>> idToEntities = docId2Entity.apply("group-by-key",
				GroupByKey.<String, T>create());
		PCollection<T> nonredundantStatusEntities = idToEntities.apply("dedup-by-key",
				ParDo.of(new DoFn<KV<String, Iterable<T>>, T>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Iterable<T> entities = c.element().getValue();
						// if there are more than one entity, we just return one
						c.output(entities.iterator().next());
					}
				}));
		return nonredundantStatusEntities;
	}

	public static <T> PCollection<KV<String, T>> deduplicateDocuments(
			PCollection<KV<ProcessingStatus, T>> statusEntityToPlainText) {

		PCollection<KV<String, T>> docIdToContent = statusEntityToPlainText.apply("status_entity-->document_id",
				ParDo.of(new DoFn<KV<ProcessingStatus, T>, KV<String, T>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						ProcessingStatus statusEntity = c.element().getKey();
						T content = c.element().getValue();
						String documentId = statusEntity.getDocumentId();
						c.output(KV.of(documentId, content));
					}
				}));

		return deduplicateDocumentsByStringKey(docIdToContent);

	}

	/**
	 * @param docIdToPlainText
	 * @return a non-redundant collection of document-id/document-content pairings
	 *         filtered using the document-ids (the String in the KV pair)
	 */
	public static <T> PCollection<KV<String, T>> deduplicateDocumentsByStringKey(
			PCollection<KV<String, T>> docIdToPlainText) {

		PCollection<KV<String, Iterable<T>>> idToPlainText = docIdToPlainText.apply("group-by-document-id",
				GroupByKey.<String, T>create());
		PCollection<KV<String, T>> nonredundantPlainText = idToPlainText.apply("deduplicate-by-document-id",
				ParDo.of(new DoFn<KV<String, Iterable<T>>, KV<String, T>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Iterable<T> texts = c.element().getValue();
						String key = c.element().getKey();
						// if there are more than one entity, we just return one
						c.output(KV.of(key, texts.iterator().next()));
					}
				}));
		return nonredundantPlainText;
	}

	/**
	 * 
	 * @param origEntity
	 * @param flagsToActivate
	 * @return an updated version of the input {@link Entity} with the specified
	 *         ProcessingStatusFlags activated (set to true)
	 */
	public static Entity updateStatusEntity(ProcessingStatus origEntity, ProcessingStatusFlag... flagsToActivate) {
		String documentId = origEntity.getDocumentId();
		Key key = DatastoreKeyUtil.createStatusKey(documentId);

		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);

		Set<String> collections = origEntity.getCollections();
		if (collections != null && !collections.isEmpty()) {
			List<Value> collectionNames = new ArrayList<Value>();
			for (String c : collections) {
				collectionNames.add(makeValue(c).build());
			}
			entityBuilder.putProperties(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS,
					makeValue(collectionNames).build());
		}
		entityBuilder.putProperties(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID, makeValue(documentId).build());

		List<String> publicationTypes = origEntity.getPublicationTypes();
		entityBuilder.putProperties(DatastoreConstants.STATUS_PROPERTY_PUBLICATION_TYPES,
				makeValue(CollectionsUtil.createDelimitedString(publicationTypes, "|")).build());

		String yearPublished = origEntity.getYearPublished();
		if (yearPublished == null || yearPublished.equals("false")) {
			yearPublished = "2155"; // max value of year in MySQL
		}
		entityBuilder.putProperties(DatastoreConstants.STATUS_PROPERTY_YEAR_PUBLISHED,
				makeValue(yearPublished).build());

		for (Entry<String, Boolean> entry : origEntity.getFlagPropertiesMap().entrySet()) {
			entityBuilder.putProperties(entry.getKey(), makeValue(entry.getValue()).build());
		}
		if (flagsToActivate != null) {
			for (ProcessingStatusFlag flag : flagsToActivate) {
				entityBuilder.putProperties(flag.getDatastoreFlagPropertyName(), makeValue(true).build());
			}
		}

		/*
		 * If there are new status flags available that did not exist when this status
		 * entity was last updated, also add them and set them to false.
		 */
		Map<String, Value> propertiesMap = entityBuilder.getPropertiesMap();
		for (ProcessingStatusFlag flag : ProcessingStatusFlag.values()) {
			if (flag != ProcessingStatusFlag.NOOP && !propertiesMap.containsKey(flag.getDatastoreFlagPropertyName())) {
				entityBuilder.putProperties(flag.getDatastoreFlagPropertyName(), makeValue(false).build());
			}
		}

		Entity entity = entityBuilder.build();
		return entity;
	}

	/**
	 * @param statusEntities
	 * @param flagsToActivate
	 * @return A collection of updated {@link Entity} objects whereby the specified
	 *         ProcessingStatusFlags have been set to true and all other flag remain
	 *         as they were. If there are new status flags available that were not
	 *         when this status entity was last updated, also add them and set them
	 *         to false.
	 */
	public static PCollection<Entity> updateStatusEntities(PCollection<ProcessingStatus> statusEntities,
			ProcessingStatusFlag... flagsToActivate) {

		return statusEntities.apply("update-status", ParDo.of(new DoFn<ProcessingStatus, Entity>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				ProcessingStatus entity = c.element();
				Entity updatedEntity = updateStatusEntity(entity, flagsToActivate);
				c.output(updatedEntity);
			}
		}));

	}

	/**
	 * Log a failure to Datastore
	 * 
	 * @param failureTag
	 * @param message
	 * @param outputDocCriteria
	 * @param timestamp
	 * @param out
	 * @param docId
	 * @param t
	 */
	public static void logFailure(TupleTag<EtlFailureData> failureTag, String message,
			DocumentCriteria outputDocCriteria, com.google.cloud.Timestamp timestamp, MultiOutputReceiver out,
			String docId, Throwable t) {

		EtlFailureData failure = initFailure(message, outputDocCriteria, timestamp, docId, t);
		out.get(failureTag).output(failure);
	}

	public static EtlFailureData initFailure(String message, DocumentCriteria outputDocCriteria,
			com.google.cloud.Timestamp timestamp, String docId, Throwable t) {

		EtlFailureData failure = (t == null) ? new EtlFailureData(outputDocCriteria, message, docId, timestamp)
				: new EtlFailureData(outputDocCriteria, message, docId, t, timestamp);
		return failure;
	}

	/**
	 * e.g. TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;OGER_CHEBI|BIONLP|OGER|0.1.0
	 * 
	 * @param inputDocumentCriteria
	 * @return
	 */
	public static Set<DocumentCriteria> compileInputDocumentCriteria(String inputDocumentCriteria) {
		String[] toks = inputDocumentCriteria.split(";");
		Set<DocumentCriteria> docCriteria = new HashSet<DocumentCriteria>();
		for (String tok : toks) {
			String[] cols = tok.split("\\|");
			DocumentType type = DocumentType.valueOf(cols[0]);
			DocumentFormat format = DocumentFormat.valueOf(cols[1]);
			PipelineKey pipeline = PipelineKey.valueOf(cols[2]);
			String pipelineVersion = cols[3];
			DocumentCriteria dc = new DocumentCriteria(type, format, pipeline, pipelineVersion);
			docCriteria.add(dc);
		}
		return docCriteria;
	}

	public static Set<ProcessingStatusFlag> compileRequiredProcessingStatusFlags(String delimitedFlags) {
		Set<ProcessingStatusFlag> flags = new HashSet<ProcessingStatusFlag>();
		for (String flagStr : delimitedFlags.split("\\|")) {
			flags.add(ProcessingStatusFlag.valueOf(flagStr));
		}
		return flags;
	}

	/**
	 * Map document type to content. This map assumes only one document in the input
	 * for each type, so if multiple documents for a given type are observed an
	 * exception is thrown.
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static Map<DocumentType, Collection<TextAnnotation>> getDocTypeToContentMap(String documentId,
			Map<DocumentCriteria, String> inputDocuments) throws IOException {
		Map<DocumentType, Collection<TextAnnotation>> docTypeToAnnotMap = new HashMap<DocumentType, Collection<TextAnnotation>>();

		// document text is needed to parse some of the document formats
		// we will check to see if the DocumentType.AUGMENTED_TEXT is present -- if it
		// is, then we will retrieve the augmented document text
		String documentText = null;
		if (containsDocumentType(inputDocuments, DocumentType.AUGMENTED_TEXT)) {
			documentText = getAugmentedDocumentText(inputDocuments, documentId);
		} else {
			documentText = getDocumentText(inputDocuments, documentId);
		}

		for (Entry<DocumentCriteria, String> entry : inputDocuments.entrySet()) {
			DocumentType documentType = entry.getKey().getDocumentType();

			// there is no handling for duplicate document types, so throw an exception if
			// duplicate types are observed
			if (docTypeToAnnotMap.containsKey(documentType)) {
				throw new IllegalArgumentException(String.format(
						"Observed multiple documents of type %s. Input documents should be unique with regard to document type.",
						documentType));
			}

			DocumentFormat documentFormat = entry.getKey().getDocumentFormat();
			String content = entry.getValue();
			if (docTypeToAnnotMap.containsKey(documentType)) {
				throw new IllegalArgumentException(
						String.format("Duplicate document type (%s) detected in input.", documentType.name()));
			}

			Collection<TextAnnotation> annots = deserializeAnnotations(documentId, content, documentFormat,
					documentText);

			if (!annots.isEmpty()) {
				docTypeToAnnotMap.put(documentType, annots);
			}

		}

		return docTypeToAnnotMap;
	}

	/**
	 * cycle through the input documents and return the document text
	 * 
	 * @param inputDocuments
	 * @return
	 */
	public static String getDocumentText(Map<DocumentCriteria, String> inputDocuments, String documentId) {
		return getDocumentByType(inputDocuments, DocumentType.TEXT, documentId);
	}

	/**
	 * The augmented document text consists of the original document text with the
	 * addition of extra sentences that have abbreviation definitions.
	 * 
	 * @param inputDocuments
	 * @return
	 */
	public static String getAugmentedDocumentText(Map<DocumentCriteria, String> inputDocuments, String documentId) {
		String originalText = getDocumentByType(inputDocuments, DocumentType.TEXT, documentId);
		String augText = getDocumentByType(inputDocuments, DocumentType.AUGMENTED_TEXT, documentId);
		return originalText + augText;
	}

	public static String getAugmentedSentenceBionlp(Map<DocumentCriteria, String> inputDocuments, String documentId) {
		String originalSentBionlp = getDocumentByType(inputDocuments, DocumentType.SENTENCE, documentId);
		String augSentBionlp = getDocumentByType(inputDocuments, DocumentType.AUGMENTED_SENTENCE, documentId);
		return originalSentBionlp + "\n" + augSentBionlp;
	}

	/**
	 * Returns true if the inputDocuments contains a document of the specified type
	 * 
	 * @param inputDocuments
	 * @param docType
	 * @return
	 */
	public static boolean containsDocumentType(Map<DocumentCriteria, String> inputDocuments, DocumentType docType) {
		for (Entry<DocumentCriteria, String> entry : inputDocuments.entrySet()) {
			DocumentCriteria documentCriteria = entry.getKey();
			if (documentCriteria.getDocumentType() == docType) {
				return true;
			}
		}
		return false;
	}

	/**
	 * cycle through the input documents and return the specified DocumentType
	 * 
	 * @param inputDocuments
	 * @return
	 */
	public static String getDocumentByType(Map<DocumentCriteria, String> inputDocuments, DocumentType docType,
			String documentId) {
		for (Entry<DocumentCriteria, String> entry : inputDocuments.entrySet()) {
			DocumentCriteria documentCriteria = entry.getKey();
			if (documentCriteria.getDocumentType() == docType) {
				return entry.getValue();
			}
		}
		LOGGER.log(Level.WARNING,
				String.format("Unable to find document type (%s) in input documents for document id: %s.",
						docType.name(), documentId));
		return "";
	}

	private static Collection<TextAnnotation> deserializeAnnotations(String documentId, String content,
			DocumentFormat documentFormat, String documentText) throws IOException {
		switch (documentFormat) {
		case BIONLP:
			BioNLPDocumentReader reader = new BioNLPDocumentReader();
			TextDocument td = reader.readDocument(documentId, "unknown", new ByteArrayInputStream(content.getBytes()),
					new ByteArrayInputStream(documentText.getBytes()), CharacterEncoding.UTF_8);
			return td.getAnnotations();
		default:
			// this is a document format that does not serialize annotations, e.g. TEXT
			return Collections.emptyList();
//			LOGGER.log(Level.)
//			throw new IllegalArgumentException(
//					String.format("Unhandled input document format: %s", documentFormat.name()));

		}
	}

	public enum FilterFlag {
		NONE, BY_CRF
	}

	/**
	 * This method returns a mapping from DocumentType (we use the concept document
	 * type, e.g., CONCEPT_CHEBI, CONCEPT_CL, etc.) to filtered concept annotations.
	 * 
	 * If the FilterFlag is CRF, then if there are both concepts and CRF annotations
	 * for a concept type, then only those concept annotations that overlap with a
	 * CRF annotation are returned. If there is are concept annotations but no CRF
	 * annotations, then the concept annotations are returned (no filtering is
	 * applied).
	 * 
	 * If the FilterFlag is NONE, then all concept annotations are returned. No
	 * filtering is applied.
	 * 
	 * @param docTypeToAnnotMap
	 * @param filterFlag
	 * @return
	 */
	public static Map<DocumentType, Set<TextAnnotation>> filterConceptAnnotations(
			Map<DocumentType, Collection<TextAnnotation>> docTypeToAnnotMap, FilterFlag filterFlag) {
		Map<DocumentType, Set<TextAnnotation>> typeToAnnotMap = new HashMap<DocumentType, Set<TextAnnotation>>();

		Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> map = pairConceptWithCrfAnnots(docTypeToAnnotMap);

		for (Entry<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> entry : map.entrySet()) {

			DocumentType type = entry.getKey();
			Set<TextAnnotation> conceptAnnots = entry.getValue().get(CrfOrConcept.CONCEPT);
			if (conceptAnnots != null) {

				if (filterFlag == FilterFlag.BY_CRF && entry.getValue().get(CrfOrConcept.CRF) != null) {
					Set<TextAnnotation> crfAnnots = entry.getValue().get(CrfOrConcept.CRF);
					Set<TextAnnotation> filteredAnnots = filterViaCrf(conceptAnnots, crfAnnots);
					if (filteredAnnots.size() > 0) {
						typeToAnnotMap.put(type, filteredAnnots);
					}
				} else if (filterFlag == FilterFlag.BY_CRF && entry.getValue().get(CrfOrConcept.CRF) == null) {
					// if the concept type is DRUGBANK or SNOMEDCT we pass it through without
					// filtering b/c we don't have a CRF for those concept types -- but if there are
					// no CRF annotations for a concept type that we do have a CRF for, then we
					// filter all concept annotations for that type.
					if (entry.getKey() == DocumentType.CONCEPT_DRUGBANK
							|| entry.getKey() == DocumentType.CONCEPT_SNOMEDCT) {
						typeToAnnotMap.put(type, conceptAnnots);
					}
				} else if (filterFlag == FilterFlag.NONE) {
					typeToAnnotMap.put(type, conceptAnnots);
				} else {
					throw new IllegalArgumentException("Unhandled FilterFlag: " + filterFlag.name());
				}
			}

		}
		return typeToAnnotMap;
	}

	/**
	 * Takes as input a collection of concept annotations and a collection of crf
	 * annotations and returns a list of concept annotations that overlap with a crf
	 * annotation.
	 * 
	 * @param conceptAnnots
	 * @param crfAnnots
	 * @return
	 */
	public static Set<TextAnnotation> filterViaCrf(Collection<TextAnnotation> conceptAnnots,
			Collection<TextAnnotation> crfAnnots) {

		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();

		// annotations must be sorted
		List<TextAnnotation> conceptList = new ArrayList<TextAnnotation>(conceptAnnots);
		Collections.sort(conceptList, TextAnnotation.BY_SPAN());
		List<TextAnnotation> crfList = new ArrayList<TextAnnotation>(crfAnnots);
		Collections.sort(crfList, TextAnnotation.BY_SPAN());

		for (TextAnnotation conceptAnnot : conceptList) {
			for (TextAnnotation crfAnnot : crfList) {
				if (conceptAnnot.getAnnotationSpanEnd() < crfAnnot.getAnnotationSpanStart()) {
					// if the concept annot ends before the crf annotation starts, then we don't
					// need to look at any more crf annotations b/c none will overlap - we can make
					// this assumption b/c the lists are sorted by span
					break;
				}
				if (conceptAnnot.overlaps(crfAnnot)) {
					toKeep.add(conceptAnnot);
					break;
				}
			}
		}

		return toKeep;
	}

	public enum CrfOrConcept {
		CRF, CONCEPT
	}

	@VisibleForTesting
	protected static Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> pairConceptWithCrfAnnots(
			Map<DocumentType, Collection<TextAnnotation>> inputMap) {

		/*
		 * outer map key is the concept type, e.g. CHEBI, CL, etc. inner map links keys:
		 * CONCEPT & CRF to the associated bionlp formatted doc content
		 */
		Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> docTypeToSplitAnnotMap = new HashMap<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>>();

		for (Entry<DocumentType, Collection<TextAnnotation>> entry : inputMap.entrySet()) {
			DocumentType documentType = entry.getKey();
			Collection<TextAnnotation> annots = entry.getValue();

			CrfOrConcept crfOrConcept = CrfOrConcept.CONCEPT;
			if (documentType.name().startsWith("CRF_")) {
				crfOrConcept = CrfOrConcept.CRF;
			}
			// we will equate the NLMDISEASE CRF annotations with MONDO and to HP so it is
			// easier to
			// match them -- use of HashSet makes for easier testing b/c order doesn't
			// matter
			if (documentType == DocumentType.CRF_NLMDISEASE) {
				addToMap(docTypeToSplitAnnotMap, new HashSet<TextAnnotation>(annots), DocumentType.CONCEPT_MONDO,
						crfOrConcept);
				addToMap(docTypeToSplitAnnotMap, new HashSet<TextAnnotation>(annots), DocumentType.CONCEPT_HP,
						crfOrConcept);
			} else if (documentType == DocumentType.CRF_CRAFT || documentType == DocumentType.CONCEPT_CIMAX
					|| documentType == DocumentType.CONCEPT_CIMIN || documentType == DocumentType.CONCEPT_CS
					|| documentType == DocumentType.CONCEPT_OGER_PP1 || documentType == DocumentType.CONCEPT_OGER_PP2) {
				// these documents have a mix of concept types, so we need to add them to the
				// map individually -- note only CONCEPT_OGER_PP2 is needed here likely but the
				// others are kept as they were used in the past before we had to split up the
				// post_processing (because the pipeline was stalling b/c the side inputs were
				// too large)
				for (TextAnnotation annot : annots) {
					DocumentType indexType = getDocumentType(annot);
					if (indexType != null) {
						addToMap(docTypeToSplitAnnotMap, new HashSet<TextAnnotation>(Arrays.asList(annot)), indexType,
								crfOrConcept);
						// add CRAFT MONDO CRF annotations to the CRF HP annotations too
						if (crfOrConcept == CrfOrConcept.CRF && indexType == DocumentType.CONCEPT_MONDO) {
							addToMap(docTypeToSplitAnnotMap, new HashSet<TextAnnotation>(Arrays.asList(annot)),
									DocumentType.CONCEPT_HP, crfOrConcept);
						}
					}
				}
			} else if (documentType.name().startsWith("CRF_") || documentType.name().startsWith("CONCEPT_")) {
				throw new IllegalArgumentException(
						String.format("Unhandled CRF or CONCEPT type (%s). Code changes needed.", documentType.name()));
			}
		}

		return docTypeToSplitAnnotMap;

	}

	private static DocumentType getDocumentType(TextAnnotation annot) {
		String idPrefix = annot.getClassMention().getMentionName().split(":")[0];
		if (idPrefix.startsWith("B-") || idPrefix.startsWith("I-")) {
			idPrefix = idPrefix.substring(2);
		}
		switch (idPrefix) {
		case "CHEBI":
			return DocumentType.CONCEPT_CHEBI;
		case "CL":
			return DocumentType.CONCEPT_CL;
		case "GO_BP":
			return DocumentType.CONCEPT_GO_BP;
		case "GO_CC":
			return DocumentType.CONCEPT_GO_CC;
		case "GO_MF":
			return DocumentType.CONCEPT_GO_MF;
		case "DRUGBANK":
			return DocumentType.CONCEPT_DRUGBANK;
		case "HP":
			return DocumentType.CONCEPT_HP;
		case "MONDO":
			return DocumentType.CONCEPT_MONDO;
		case "NCBITaxon":
			return DocumentType.CONCEPT_NCBITAXON;
		case "PR":
			return DocumentType.CONCEPT_PR;
		case "SNOMEDCT":
			return DocumentType.CONCEPT_SNOMEDCT;
		case "SO":
			return DocumentType.CONCEPT_SO;
		case "UBERON":
			return DocumentType.CONCEPT_UBERON;
		case "TMKPUTIL":
			return null;

		default:
			throw new IllegalArgumentException(
					String.format("Unhandled concept id prefix (%s). Cannot convert to DocumentType.", idPrefix));
		}
	}

	/**
	 * Add a document of a given type to the map
	 * 
	 * @param conceptTypeToContentMap
	 * @param documentContent
	 * @param type
	 * @param crfOrConcept
	 */
	@VisibleForTesting
	protected static void addToMap(Map<DocumentType, Map<CrfOrConcept, Set<TextAnnotation>>> conceptTypeToContentMap,
			Set<TextAnnotation> annots, DocumentType type, CrfOrConcept crfOrConcept) {
		if (conceptTypeToContentMap.containsKey(type)) {
			if (conceptTypeToContentMap.get(type).containsKey(crfOrConcept)) {
				conceptTypeToContentMap.get(type).get(crfOrConcept).addAll(annots);
			} else {
				conceptTypeToContentMap.get(type).put(crfOrConcept, annots);
			}
		} else {
			Map<CrfOrConcept, Set<TextAnnotation>> innerMap = new HashMap<CrfOrConcept, Set<TextAnnotation>>();
			innerMap.put(crfOrConcept, annots);
			conceptTypeToContentMap.put(type, innerMap);
		}
	}

	/**
	 * Given a map that contains Collections as values, return a set that is the
	 * aggregate of all unique collection members
	 * 
	 * @param <T>
	 * @param map
	 * @return
	 */
	public static <T> Set<T> spliceValues(Map<String, Collection<T>> map) {
		Set<T> set = new HashSet<T>();
		for (Collection<T> collection : map.values()) {
			for (T t : collection) {
				set.add(t);
			}
		}
		return set;
	}

	public static <T> Set<T> spliceValues(Collection<Set<T>> collections) {
		Set<T> set = new HashSet<T>();
		for (Collection<T> collection : collections) {
			for (T t : collection) {
				set.add(t);
			}
		}
		return set;
	}

	public static TextAnnotation clone(TextAnnotation annot) {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(annot.getDocumentID());
		List<Span> origSpans = annot.getSpans();
		Span firstSpan = origSpans.get(0);
		TextAnnotation ta = factory.createAnnotation(firstSpan.getSpanStart(), firstSpan.getSpanEnd(),
				annot.getCoveredText(), annot.getClassMention().getMentionName());
		for (int i = 1; i < origSpans.size(); i++) {
			ta.addSpan(new Span(origSpans.get(i).getSpanStart(), origSpans.get(i).getSpanEnd()));
		}
		return ta;
	}

	/**
	 * Returns a collection of existing document identifiers, unless the
	 * OverwriteOutput flag is set to YES, in which case it returns an empty set.
	 * 
	 * @param options
	 * @param p
	 * @return
	 */
	public static PCollectionView<Set<String>> catalogExistingDocuments(String project, String collection,
			OverwriteOutput overwrite, Pipeline p) {
		/*
		 * if OverwriteOutput == YES, then there is no need to catalog the existing
		 * documents as they will be overwritten if they exist
		 */
		if (overwrite == OverwriteOutput.YES) {
			return p.apply("Create schema view", Create.<Set<String>>of(CollectionsUtil.createSet("")))
					.apply(View.<Set<String>>asSingleton());
		}

		// catalog documents that have already been stored so that we can exclude
		// loading redundant documents. There will be cases in the PubMed/Medline update
		// files where records are included due to changes, e.g. modification date, but
		// are not new documents, so they should be excluded from being loaded into
		// datastore here.

		PCollection<KV<String, ProcessingStatus>> docIdToStatusEntity = PipelineMain.getStatusEntitiesToProcess(p,
				ProcessingStatusFlag.NOOP, CollectionsUtil.createSet(ProcessingStatusFlag.TEXT_DONE), project,
				collection, OverwriteOutput.YES);

		// create a set of document IDs already present in Datastore to be used as a
		// side input
		PCollection<String> docIds = docIdToStatusEntity.apply(Keys.<String>create());
		final PCollectionView<Set<String>> existingDocumentIds = PCollectionUtil.createPCollectionViewSet(docIds);
		return existingDocumentIds;
	}

//	/**
//	 * @param statusEntity
//	 * @return a mapping from document ID to collection names
//	 */
//	public static PCollection<KV<String, Set<String>>> getCollectionMappings(
//			PCollection<ProcessingStatus> statusEntity) {
//		return statusEntity.apply(ParDo.of(new DoFn<ProcessingStatus, KV<String, Set<String>>>() {
//			private static final long serialVersionUID = 1L;
//
//			@ProcessElement
//			public void processElement(ProcessContext c) {
//				ProcessingStatus ps = c.element();
//				c.output(KV.of(ps.getDocumentId(), ps.getCollections()));
//			}
//		}));
//
//	}

	public static PCollection<KV<String, Set<String>>> getCollectionMappings(
			PCollection<Entity> nonredundantStatusEntities) {
		return nonredundantStatusEntities.apply(ParDo.of(new DoFn<Entity, KV<String, Set<String>>>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				Entity statusEntity = c.element();
				ProcessingStatus ps = new ProcessingStatus(statusEntity);
				c.output(KV.of(ps.getDocumentId(), ps.getCollections()));
			}
		}));
	}

	/**
	 * This method is used to test if all of the required documents are present. It
	 * allows for flexible matching of the pipeline version in case the pipeline
	 * version is set to "recent".
	 * 
	 * @param docCriteria
	 * @param requiredDocumentCriteria
	 * @return true if the input docCriteria fulfills the requiredDocumentCriteria.
	 *         This doesn't need to be an exact match, b/c the required-doc-criteria
	 *         may use "recent" as the pipeline version, where as the docCriteria
	 *         has been extracted from Datastore and should have an explicit
	 *         version. So we will check for an exact match if the pipeline version
	 *         is specified as required, or a more loose match if the pipeline
	 *         version is declared as 'recent'.
	 */
	@VisibleForTesting
	public static boolean fulfillsRequiredDocumentCriteria(Set<DocumentCriteria> docCriteria,
			Set<DocumentCriteria> requiredDocumentCriteria) {

		Set<String> pipelineVersionAgnosticDocCriteria = new HashSet<String>();
		for (DocumentCriteria reqDc : docCriteria) {
			pipelineVersionAgnosticDocCriteria.add(PipelineMain.createVersionAgnosticKey(reqDc));
		}

		for (DocumentCriteria reqDc : requiredDocumentCriteria) {
			String pipelineVersion = reqDc.getPipelineVersion();
			if (pipelineVersion.equalsIgnoreCase(PIPELINE_VERSION_RECENT)) {
				String key = PipelineMain.createVersionAgnosticKey(reqDc);
				if (!pipelineVersionAgnosticDocCriteria.contains(key)) {
					return false;
				}
			} else {
				if (!docCriteria.contains(reqDc)) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Checks to see if required documents are present. Returns true if they are.
	 * Logs an error if they are not.
	 * 
	 * @param docCriteria
	 * @param requiredDocumentCriteria
	 * @param pipelineKey
	 * @return
	 */
	public static boolean requiredDocumentsArePresent(Set<DocumentCriteria> docCriteria,
			Set<DocumentCriteria> requiredDocumentCriteria, PipelineKey pipelineKey,
			TupleTag<EtlFailureData> failureTag, String documentId, DocumentCriteria errorDocCriteria,
			Timestamp timestamp, MultiOutputReceiver out) {
		if (!PipelineMain.fulfillsRequiredDocumentCriteria(docCriteria, requiredDocumentCriteria)) {
			PipelineMain.logFailure(failureTag, String.format(
					"Unable to complete processing due to missing annotation documents for: %s. Observed (%d): %s, but requires (%d): %s.",
					documentId, docCriteria.size(), docCriteria.toString(), requiredDocumentCriteria.size(),
					requiredDocumentCriteria.toString()), errorDocCriteria, timestamp, out, documentId, null);
			return false;
		}
		return true;
	}

}
