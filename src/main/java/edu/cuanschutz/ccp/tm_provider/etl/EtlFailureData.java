package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFn;

import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Modified from:
 * https://github.com/vllry/beam-errorhandle-example/blob/master/src/main/java/errorfilteringdemo/datum/Failure.java
 */
@Data
@EqualsAndHashCode(callSuper = false)
@SuppressWarnings("rawtypes")
public class EtlFailureData extends DoFn {

	private static final long serialVersionUID = 1L;

	private final String message;
	private final String documentId;
	private final String stackTrace;
	private final DocumentCriteria documentCriteria;
	private final com.google.cloud.Timestamp timestamp;

	public EtlFailureData(DocumentCriteria documentCriteria, String customMessage, String documentId, Throwable thrown,
			com.google.cloud.Timestamp timestamp) {
		this.documentCriteria = documentCriteria;
		this.message = trimMessage(customMessage + " -- " + thrown.toString());
		this.documentId = documentId;
		this.stackTrace = Arrays.toString(thrown.getStackTrace());
		this.timestamp = timestamp;
	}

	public EtlFailureData(DocumentCriteria documentCriteria, String customMessage, String documentId,
			com.google.cloud.Timestamp timestamp) {
		this.documentCriteria = documentCriteria;
		this.message = trimMessage(customMessage);
		this.documentId = documentId;
		this.stackTrace = "";
		this.timestamp = timestamp;
	}

	private String trimMessage(String message) {
		String updatedMessage = message;
		try {
			while (updatedMessage.getBytes("UTF-8").length > 1400) {
				updatedMessage = updatedMessage.substring(0, updatedMessage.length() - 2);
			}
		} catch (UnsupportedEncodingException e) {
			if (updatedMessage.length() > 1000) {
				updatedMessage = updatedMessage.substring(0, 1000);
			}
		}
		return updatedMessage;
	}

}
