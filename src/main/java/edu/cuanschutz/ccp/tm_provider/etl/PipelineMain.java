package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.Int32Value;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.file.CharacterEncoding;

public class PipelineMain {

	private final static Logger LOGGER = Logger.getLogger(PipelineMain.class.getName());

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
			case ADD_SUB_COLLECTION:
				AddSubCollectionPipeline.main(pipelineArgs);
				break;
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
	public static PCollection<KV<Entity, Map<DocumentCriteria, String>>> getStatusEntity2Content(
			List<DocumentCriteria> inputDocCriteria, String gcpProjectId, Pipeline beamPipeline,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String collection, OverwriteOutput overwriteOutput, int queryLimit) {

		/*
		 * get the status entities for documents that meet the required process status
		 * flag critera but whose target process status flag is false
		 */
		PCollection<Entity> status = getStatusEntitiesToProcess(beamPipeline, targetProcessStatusFlag,
				requiredProcessStatusFlags, gcpProjectId, collection, overwriteOutput, queryLimit);

		/*
		 * then return a mapping from document id to the document content that will be
		 * processed
		 */
		PCollection<KV<Entity, Map<DocumentCriteria, String>>> statusEntity2Content = status
				.apply("get document content", ParDo.of(new DoFn<Entity, KV<Entity, Map<DocumentCriteria, String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element Entity statusEntity,
							OutputReceiver<KV<Entity, Map<DocumentCriteria, String>>> out) {
//						String documentId = statusEntity.getPropertiesMap().get(STATUS_PROPERTY_DOCUMENT_ID).getStringValue();

						// TODO: This needs to be fixed -- this chunk count field is only present for
						// the text document right now
//						String chunkCountPropertyName = DatastoreProcessingStatusUtil
//								.getDocumentChunkCountPropertyName(inputDocCriteria);
//						long chunkCount = status.getPropertiesMap().get(chunkCountPropertyName).getIntegerValue();

						DatastoreDocumentUtil util = new DatastoreDocumentUtil();
						KV<Entity, Map<DocumentCriteria, String>> documentIdToContent = util
								.getStatusEntityToContent(statusEntity, inputDocCriteria);
						if (documentIdToContent != null) {
							out.output(documentIdToContent);
						}
					}
				}));
		return statusEntity2Content;
	}

	public static PCollection<Entity> getStatusEntitiesToProcess(Pipeline p,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String gcpProjectId, String collection, OverwriteOutput overwriteOutput, Integer queryLimit) {
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
			LOGGER.log(Level.INFO, "FILTER: " + filter.toString());
		}

		Filter filter = makeAndFilter(filters).build();
		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(STATUS_KIND);
		query.setFilter(filter);

		if (queryLimit != null && queryLimit != 0) {
			query.setLimit(Int32Value.of(queryLimit));

		}

		PCollection<Entity> status = p.apply("datastore->status entities to process",
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(gcpProjectId));
		return status;
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

	/**
	 * @param statusEntities
	 * @return a non-redundant collection of the input entities filtered based on
	 *         the entity keys (the String in the KV pair)
	 */
	public static PCollection<Entity> deduplicateEntitiesByKey(PCollection<KV<String, Entity>> statusEntities) {
		// remove any duplicates
		PCollection<KV<String, Iterable<Entity>>> idToEntities = statusEntities.apply("group-by-key",
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
			PCollection<KV<Entity, List<String>>> statusEntityToPlainText) {

		PCollection<KV<String, List<String>>> docIdToContent = statusEntityToPlainText.apply(
				"status_entity-->document_id", ParDo.of(new DoFn<KV<Entity, List<String>>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						Entity statusEntity = c.element().getKey();
						List<String> content = c.element().getValue();
						String documentId = DatastoreProcessingStatusUtil.getDocumentId(statusEntity);
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
	private static Entity updateStatusEntity(Entity origEntity, ProcessingStatusFlag... flagsToActivate) {
		Key key = origEntity.getKey();

		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);

		for (Entry<String, Value> entry : origEntity.getPropertiesMap().entrySet()) {
			entityBuilder.putProperties(entry.getKey(), entry.getValue());
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
	public static PCollection<Entity> updateStatusEntities(PCollection<Entity> statusEntities,
			ProcessingStatusFlag... flagsToActivate) {

		return statusEntities.apply("update-status", ParDo.of(new DoFn<Entity, Entity>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				Entity entity = c.element();
				Entity updatedEntity = updateStatusEntity(entity, flagsToActivate);
				c.output(updatedEntity);
			}
		}));

	}

}
