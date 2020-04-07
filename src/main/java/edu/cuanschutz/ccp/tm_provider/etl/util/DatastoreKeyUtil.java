package edu.cuanschutz.ccp.tm_provider.etl.util;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.FAILURE_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;

public class DatastoreKeyUtil {
	
	private final static Logger LOGGER = Logger.getLogger(DatastoreKeyUtil.class.getName());

	///////////////////////////////
	//////// DOCUMENT KEY /////////
	///////////////////////////////

	public static String getDocumentKeyName(String docId, DocumentCriteria dc) {
		return String.format("%s.%s.%s.%s.%s", docId, dc.getDocumentType().name().toLowerCase(),
				dc.getDocumentFormat().name().toLowerCase(), dc.getPipelineKey().name().toLowerCase(),
				dc.getPipelineVersion());
	}

	public static Key createDocumentKey(String documentId, Long chunkId, DocumentCriteria dc) {
		String docName = getDocumentKeyName(documentId + "." + chunkId, dc);
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

	public static Key createFailureKey(String docId, DocumentCriteria dc) {
		String docName = String.format("%s.%s.%s.%s.%s", docId, dc.getPipelineKey().name().toLowerCase(),
				dc.getPipelineVersion(), dc.getDocumentType().name(), dc.getDocumentFormat().name());
		Builder builder = Key.newBuilder();
		PathElement pathElement = builder.addPathBuilder().setKind(FAILURE_KIND).setName(docName).build();
		Key key = builder.setPath(0, pathElement).build();
		return key;
	}

}
