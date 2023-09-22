package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CHUNK_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CHUNK_TOTAL;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_FORMAT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreKeyUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.SerializableFunction;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.string.StringUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Transforms a PCollection that maps from document ID to document text to
 * PCollection containing Google Cloud Datastore Entities
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DocumentToEntityFn extends DoFn<KV<String, List<String>>, Entity> {

	private static final long serialVersionUID = 1L;
	private final DocumentCriteria dc;
	private final String collection;
	private final SerializableFunction<String, String> collectionFn;
	private final PCollectionView<Map<String, Set<String>>> documentIdToCollectionsMapView;

	public DocumentToEntityFn(DocumentCriteria dc, String collection, SerializableFunction<String, String> collectionFn,
			PCollectionView<Map<String, Set<String>>> documentIdToCollectionsMapView) {
		this.dc = dc;
		this.collection = collection;
		this.collectionFn = collectionFn;
		this.documentIdToCollectionsMapView = documentIdToCollectionsMapView;
	}

	public DocumentToEntityFn(DocumentCriteria dc, String collection,
			PCollectionView<Map<String, Set<String>>> documentIdToCollectionsMapView) {
		this.dc = dc;
		this.collection = collection;
		this.collectionFn = null;
		this.documentIdToCollectionsMapView = documentIdToCollectionsMapView;
	}

	@ProcessElement
//	public void processElement(@Element KV<String, List<String>> docIdToDocContent, OutputReceiver<Entity> out)
	public void processElement(ProcessContext c) throws UnsupportedEncodingException {
		String docId = c.element().getKey();
		List<String> docContentChunks = c.element().getValue();

		Map<String, Set<String>> docIdToCollectionsMap = c.sideInput(documentIdToCollectionsMapView);

		/*
		 * TODO: note, we are passing in the collections from the ProcessingStatus
		 * entity. This might negate the need for the collection function argument. It
		 * may be worth revisiting this at some point.
		 */
		Set<String> extraCollections = docIdToCollectionsMap.get(docId);

		/* crop document id if it is a file path */
		docId = updateDocId(docId);

		Set<String> collections = ToEntityFnUtils.getCollections(collection, collectionFn, docId);
		if (extraCollections != null) {
			collections.addAll(extraCollections);
		}

		int index = 0;
		for (String docContent : docContentChunks) {
			Entity entity = createEntity(docId, index++, docContentChunks.size(), dc, docContent, collections);
			c.output(entity);
		}

	}

	/**
	 * if the document id is a file path, then extract the file name. The BioC file
	 * names contain PMC IDs without the PMC, e.g. 17299597.xml. After the file name
	 * is extracted, append "PMC" as a prefix to match downstream document
	 * identifiers.
	 * 
	 * @param docId
	 * @return
	 */
	protected static String updateDocId(String docId) {
		if (docId.contains("/")) {
			String updatedDocId = docId.substring(docId.lastIndexOf("/") + 1);
			/* remove file suffix if there is one */
			if (StringUtil.endsWithRegex(updatedDocId, "\\..+")) {
				updatedDocId = StringUtil.removeSuffixRegex(updatedDocId, "\\..+");
			}
			/* if the id is all digits, then we assume it's a PMC id, so append "PMC" */
			if (updatedDocId.matches("\\d+")) {
				updatedDocId = "PMC" + updatedDocId;
			}
			return updatedDocId;
		}
		return docId;
	}

	protected static Entity createEntity(String docId, long chunkId, int chunkTotal, DocumentCriteria dc,
			String docContent, Set<String> collectionNames) throws UnsupportedEncodingException {
		com.google.datastore.v1.Key key = DatastoreKeyUtil.createDocumentKey(docId, chunkId, dc);

		/*
		 * the document content is likely too large to store as a property, so we make
		 * it a blob and store it unindexed
		 */
		ByteString docContentBlob = ByteString.copyFrom(docContent, CharacterEncoding.UTF_8.getCharacterSetName());
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(DOCUMENT_PROPERTY_ID, makeValue(docId).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_CHUNK_ID, makeValue(chunkId).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_CHUNK_TOTAL, makeValue(chunkTotal).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_CONTENT,
				makeValue(docContentBlob).setExcludeFromIndexes(true).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_FORMAT, makeValue(dc.getDocumentFormat().name()).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_TYPE, makeValue(dc.getDocumentType().name()).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_PIPELINE_VERSION, makeValue(dc.getPipelineVersion()).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_PIPELINE, makeValue(dc.getPipelineKey().name()).build());

		List<Value> collections = new ArrayList<Value>();

		List<String> sortedCollectionNames = new ArrayList<String>(collectionNames);
		Collections.sort(sortedCollectionNames);

		for (String collection : sortedCollectionNames) {
			collections.add(makeValue(collection).build());
		}
		entityBuilder.putProperties(DOCUMENT_PROPERTY_COLLECTIONS, makeValue(collections).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
