package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@SuppressWarnings("rawtypes")
@EqualsAndHashCode(callSuper = false)
public class ProcessedDocument extends DoFn {

	private static final long serialVersionUID = 1L;

	private final String documentId;

	private final DocumentCriteria documentCriteria;

	private final String documentContent;

	private final long chunkId;

	private final long chunkTotal;

	private final Set<String> collections;

	public ProcessedDocument(String documentId, DocumentCriteria docCriteria, String documentContent, long chunkId,
			long chunkTotal, Set<String> collections) {
		this.documentId = documentId;
		this.documentCriteria = docCriteria;
		this.documentContent = documentContent;
		this.chunkId = chunkId;
		this.chunkTotal = chunkTotal;
		this.collections = new HashSet<String>(collections);
	}

	public ProcessedDocument(Entity documentEntity) throws UnsupportedEncodingException {

		this.chunkId = documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_CHUNK_ID)
				.getIntegerValue();
		this.chunkTotal = documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_CHUNK_TOTAL)
				.getIntegerValue();
		this.documentContent = documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_CONTENT)
				.getBlobValue().toString(CharacterEncoding.UTF_8.getCharacterSetName());
		this.documentId = documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_ID)
				.getStringValue();
		DocumentFormat format = DocumentFormat.valueOf(
				documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_FORMAT).getStringValue());
		DocumentType type = DocumentType.valueOf(
				documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_TYPE).getStringValue());
		PipelineKey pipelineKey = PipelineKey.valueOf(
				documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE).getStringValue());
		String pipelineVersion = documentEntity.getPropertiesMap()
				.get(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION).getStringValue();
		this.documentCriteria = new DocumentCriteria(type, format, pipelineKey, pipelineVersion);

		this.collections = new HashSet<String>();
		Value value = documentEntity.getPropertiesMap().get(DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS);
		if (value != null) {
			List<Value> collectionNames = DatastoreHelper.getList(value);
			for (Value v : collectionNames) {
				this.collections.add(v.getStringValue());
			}
		}

	}

}
