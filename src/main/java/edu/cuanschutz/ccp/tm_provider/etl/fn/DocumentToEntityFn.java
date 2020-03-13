package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_FORMAT;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_ID;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_TYPE;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.PIPELINE_KEY;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.PIPELINE_VERSION;

import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;
import com.google.protobuf.ByteString;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
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
public class DocumentToEntityFn extends DoFn<KV<String, String>, Entity> {

	private static final long serialVersionUID = 1L;
	private final DocumentType type;
	private final DocumentFormat format;
	private final PipelineKey pipeline;
	private final String pipelineVersion;

	@ProcessElement
	public void processElement(@Element KV<String, String> docIdToDocContent, OutputReceiver<Entity> out)
			throws UnsupportedEncodingException {
		String docId = docIdToDocContent.getKey();
		String docContent = docIdToDocContent.getValue();

		/* crop document id if it is a file path */
		docId = updateDocId(docId);

		Entity entity = createEntity(docId, type, format, pipeline, pipelineVersion, docContent);
		out.output(entity);

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
	static String updateDocId(String docId) {
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

	static Entity createEntity(String docId, DocumentType type, DocumentFormat format, PipelineKey pipeline,
			String pipelineVersion, String docContent) throws UnsupportedEncodingException {
		String docName = DatastoreDocumentUtil.getDocumentKeyName(docId, type, format, pipeline, pipelineVersion);
		Builder builder = Key.newBuilder();
		PathElement pathElement = builder.addPathBuilder().setKind(DOCUMENT_KIND).setName(docName).build();
		Key key = builder.setPath(0, pathElement).build();

		/*
		 * the document content is likely too large to store as a property, so we make
		 * it a blob and store it unindexed
		 */
		ByteString docContentBlob = ByteString.copyFrom(docContent, CharacterEncoding.UTF_8.getCharacterSetName());
		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);
		entityBuilder.putProperties(DOCUMENT_PROPERTY_ID, makeValue(docId).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_CONTENT,
				makeValue(docContentBlob).setExcludeFromIndexes(true).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_FORMAT, makeValue(format.name()).build());
		entityBuilder.putProperties(DOCUMENT_PROPERTY_TYPE, makeValue(type.name()).build());
		entityBuilder.putProperties(PIPELINE_VERSION, makeValue(pipelineVersion).build());
		entityBuilder.putProperties(PIPELINE_KEY, makeValue(pipeline.name()).build());

		Entity entity = entityBuilder.build();
		return entity;
	}

}
