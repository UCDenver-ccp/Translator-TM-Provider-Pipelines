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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;

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

public class PipelineMain {

	private final static Logger LOGGER = Logger.getLogger(PipelineMain.class.getName());

	private static final TupleTag<ProcessingStatus> statusTag = new TupleTag<>();
	private static final TupleTag<ProcessedDocument> documentTag = new TupleTag<>();

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
			case BIOC_TO_TEXT:
				BiocToTextPipeline.main(pipelineArgs);
				break;
			case CRF:
				CrfNerPipeline.main(pipelineArgs);
				break;
			case MEDLINE_XML_TO_TEXT:
				MedlineXmlToTextPipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE:
				DependencyParsePipeline.main(pipelineArgs);
				break;
			case FILE_LOAD:
				LoadFilesPipeline.main(pipelineArgs);
				break;
			case OGER:
				OgerPipeline.main(pipelineArgs);
				break;
			case BIGQUERY_EXPORT:
				BigQueryExportPipeline.main(pipelineArgs);
				break;
			case SENTENCE_SEGMENTATION:
				SentenceSegmentationPipeline.main(pipelineArgs);
				break;
			case SENTENCE_COOCCURRENCE_EXPORT:
				SentenceCooccurrencePipeline.main(pipelineArgs);
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

	/**
	 * @param inputDocCriteria
	 * @param gcpProjectId
	 * @param beamPipeline
	 * @param targetProcessStatusFlag
	 * @param requiredProcessStatusFlags
	 * @param collection
	 * @param overwriteOutput
	 * @return a mapping from the status entity to various document content (a
	 *         mapping from the document criteria to the document content)
	 */
	public static PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> getStatusEntity2Content(
			Set<DocumentCriteria> inputDocCriteria, String gcpProjectId, Pipeline beamPipeline,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String collection, OverwriteOutput overwriteOutput, int queryLimit) {

		/*
		 * get the status entities for documents that meet the required process status
		 * flag critera but whose target process status flag is false
		 */
		PCollection<KV<String, ProcessingStatus>> docId2Status = getStatusEntitiesToProcess(beamPipeline,
				targetProcessStatusFlag, requiredProcessStatusFlags, gcpProjectId, collection, overwriteOutput,
				queryLimit);

		KeyedPCollectionTuple<String> tuple = KeyedPCollectionTuple.of(statusTag, docId2Status);

		for (DocumentCriteria docCriteria : inputDocCriteria) {
			PCollection<KV<String, ProcessedDocument>> docId2Document = getDocumentEntitiesToProcess(beamPipeline,
					docCriteria, collection, gcpProjectId);
			tuple = tuple.and(documentTag, docId2Document);
		}

		PCollection<KV<String, CoGbkResult>> result = tuple.apply(CoGroupByKey.create());

		PCollection<KV<ProcessingStatus, Map<DocumentCriteria, String>>> outputPCollection = result.apply(
				ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<ProcessingStatus, Map<DocumentCriteria, String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> element = c.element();
						CoGbkResult result = element.getValue();

						// get the processing status -- there will only be one
						try {
							ProcessingStatus processingStatus = result.getOnly(statusTag);

							if (processingStatus != null) {
								// get all associated documents -- shere should be only one
								// Iterable<ProcessedDocument> associated with the documentTag
								Iterable<ProcessedDocument> documents = result.getAll(documentTag);

								// piece together documents that have been split for storage
								Map<DocumentCriteria, String> contentMap = spliceDocumentChunks(documents);

								c.output(KV.of(processingStatus, contentMap));
							}
						} catch (IllegalArgumentException e) {
//							throw new IllegalArgumentException(
//									"Status or documents missing for id: " + element.getKey(), e);
							LOGGER.log(Level.WARNING, "Skipping processing due to missing documents for id: " + element.getKey());
							// TODO: this should probably output an EtlFailure here instead of logging a warning
						}
					}

				}));

		return outputPCollection;
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
			String gcpProjectId, String collection, OverwriteOutput overwriteOutput, Integer queryLimit) {
		LOGGER.log(Level.INFO, String.format("PROCESSING STATUS FILTER SETTINGS: \nOVERWRITE: %s\nCOLLECTION: %s",
				overwriteOutput.name(), collection));
		List<Filter> filters = new ArrayList<Filter>();
		for (ProcessingStatusFlag flag : requiredProcessStatusFlags) {
			Filter filter = makeFilter(flag.getDatastoreFlagPropertyName(), PropertyFilter.Operator.EQUAL,
					makeValue(true)).build();
			filters.add(filter);
		}

		if (overwriteOutput == OverwriteOutput.NO) {
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

		PCollection<Entity> status = p.apply("datastore->status entities to process",
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

	public static PCollection<KV<String, ProcessedDocument>> getDocumentEntitiesToProcess(Pipeline p,
			DocumentCriteria docCriteria, String collection, String gcpProjectId) {
		List<Filter> filters = new ArrayList<Filter>();
		DocumentFormat documentFormat = docCriteria.getDocumentFormat();
		DocumentType documentType = docCriteria.getDocumentType();
		PipelineKey pipelineKey = docCriteria.getPipelineKey();
		String pipelineVersion = docCriteria.getPipelineVersion();

		if (documentFormat != null) {
			Filter filter = makeFilter(DatastoreConstants.DOCUMENT_PROPERTY_FORMAT, PropertyFilter.Operator.EQUAL,
					makeValue(documentFormat.name())).build();
			filters.add(filter);
		}

		if (documentType != null) {
			Filter filter = makeFilter(DatastoreConstants.DOCUMENT_PROPERTY_TYPE, PropertyFilter.Operator.EQUAL,
					makeValue(documentType.name())).build();
			filters.add(filter);
		}

		if (pipelineKey != null) {
			Filter filter = makeFilter(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE, PropertyFilter.Operator.EQUAL,
					makeValue(pipelineKey.name())).build();
			filters.add(filter);
		}

		if (pipelineVersion != null) {
			Filter filter = makeFilter(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION,
					PropertyFilter.Operator.EQUAL, makeValue(pipelineVersion)).build();
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

		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(DOCUMENT_KIND);

		if (!filters.isEmpty()) {
			Filter filter = makeAndFilter(filters).build();
			query.setFilter(filter);
		}

		PCollection<Entity> documents = p.apply("datastore->document entities to process",
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
	public static PCollection<Entity> deduplicateEntitiesByKey(PCollection<KV<String, Entity>> docId2Entity) {
		// remove any duplicates
		PCollection<KV<String, Iterable<Entity>>> idToEntities = docId2Entity.apply("group-by-key",
				GroupByKey.<String, Entity>create());
		PCollection<Entity> nonredundantStatusEntities = idToEntities.apply("dedup-by-key",
				ParDo.of(new DoFn<KV<String, Iterable<Entity>>, Entity>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Iterable<Entity> entities = c.element().getValue();
						// if there are more than one entity, we just return one
						c.output(entities.iterator().next());
					}
				}));
		return nonredundantStatusEntities;
	}

	public static PCollection<KV<String, List<String>>> deduplicateDocuments(
			PCollection<KV<ProcessingStatus, List<String>>> statusEntityToPlainText) {

		PCollection<KV<String, List<String>>> docIdToContent = statusEntityToPlainText.apply(
				"status_entity-->document_id",
				ParDo.of(new DoFn<KV<ProcessingStatus, List<String>>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						ProcessingStatus statusEntity = c.element().getKey();
						List<String> content = c.element().getValue();
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
	public static PCollection<KV<String, List<String>>> deduplicateDocumentsByStringKey(
			PCollection<KV<String, List<String>>> docIdToPlainText) {

		PCollection<KV<String, Iterable<List<String>>>> idToPlainText = docIdToPlainText.apply("group-by-document-id",
				GroupByKey.<String, List<String>>create());
		PCollection<KV<String, List<String>>> nonredundantPlainText = idToPlainText.apply("deduplicate-by-document-id",
				ParDo.of(new DoFn<KV<String, Iterable<List<String>>>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Iterable<List<String>> texts = c.element().getValue();
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
	@VisibleForTesting
	private static Entity updateStatusEntity(ProcessingStatus origEntity, ProcessingStatusFlag... flagsToActivate) {
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

		for (Entry<String, Boolean> entry : origEntity.getFlagPropertiesMap().entrySet()) {
			entityBuilder.putProperties(entry.getKey(), makeValue(entry.getValue()).build());
		}
		for (ProcessingStatusFlag flag : flagsToActivate) {
			entityBuilder.putProperties(flag.getDatastoreFlagPropertyName(), makeValue(true).build());
		}
		Entity entity = entityBuilder.build();
		return entity;
	}

	/**
	 * @param statusEntities
	 * @param flagsToActivate
	 * @return A collection of updated {@link Entity} objects whereby the specified
	 *         ProcessingStatusFlags have been set to true and all other flag remain
	 *         as they were.
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

}
