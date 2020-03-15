package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;

public class DatastoreKeyUtil {

	///////////////////////////////
	//////// DOCUMENT KEY /////////
	///////////////////////////////

	public static String getDocumentKeyName(String docId, DocumentType type, DocumentFormat format,
			PipelineKey pipeline, String version) {
		return String.format("%s.%s.%s.%s.%s", docId, type.name().toLowerCase(), format.name().toLowerCase(),
				pipeline.name().toLowerCase(), version);
	}

	public static Key createDocumentKey(String documentId, DocumentType type, DocumentFormat format,
			PipelineKey pipeline, String pipelineVersion) {
		String docName = getDocumentKeyName(documentId, type, format, pipeline, pipelineVersion);
		Builder builder = Key.newBuilder();
		PathElement statusElement = builder.addPathBuilder().setKind(STATUS_KIND).setName(getStatusKeyName(documentId))
				.build();
		PathElement documentElement = builder.addPathBuilder().setKind(DOCUMENT_KIND).setName(docName).build();
		Key key = builder.setPath(0, statusElement).setPath(1, documentElement).build();
		return key;
	}

	///////////////////////////////
	///////// STATUS KEY //////////
	///////////////////////////////

	public static String getStatusKeyName(String docId) {
		return String.format("%s.status", docId);
	}

	public static Key createStatusKey(String documentId) {
		String docName = getStatusKeyName(documentId);
		Builder builder = Key.newBuilder();
		PathElement pathElement = builder.addPathBuilder().setKind(STATUS_KIND).setName(docName).build();
		Key key = builder.setPath(0, pathElement).build();
		return key;
	}

	///////////////////////////////
	//////// FAILURE KEY /////////
	///////////////////////////////

	public static Key createFailureKey(PipelineKey pipeline, String pipelineVersion, DocumentType documentType,
			String docId) {
		String docName = String.format("%s.%s.%s.%s", docId, pipeline.name().toLowerCase(), pipelineVersion,
				documentType.name());
		Builder builder = Key.newBuilder();
		PathElement pathElement = builder.addPathBuilder().setKind(FAILURE_KIND).setName(docName).build();
		Key key = builder.setPath(0, pathElement).build();
		return key;
	}

}
