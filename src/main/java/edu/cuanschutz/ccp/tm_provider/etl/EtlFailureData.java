package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

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
	private static final Logger logger = org.apache.log4j.Logger.getLogger(EtlFailureData.class);

	private final String message;
	private final String documentId;
	private final String stackTrace;
	private final String causeStackTrace;
	private final String causeMessage;
	private final DocumentCriteria documentCriteria;
	private final com.google.cloud.Timestamp timestamp;

	public EtlFailureData(DocumentCriteria documentCriteria, String customMessage, String documentId, Throwable thrown,
			com.google.cloud.Timestamp timestamp) {
		this.documentCriteria = documentCriteria;
		this.message = (thrown == null) ? trimMessage(customMessage) : trimMessage(customMessage + " -- " + thrown.toString());
		this.documentId = documentId;
		this.stackTrace = (thrown == null) ? "" : Arrays.toString(thrown.getStackTrace());
		this.timestamp = timestamp;
		Throwable cause = thrown.getCause();
		this.causeMessage = (cause == null) ? "" : cause.getMessage();
		this.causeStackTrace = (cause == null) ? "" : Arrays.toString(cause.getStackTrace());
		
		logger.warn("TMPLOG -- Logging failure: " + getMessage() + " -- " + getStackTrace());
	}

	public EtlFailureData(DocumentCriteria documentCriteria, String customMessage, String documentId,
			com.google.cloud.Timestamp timestamp) {
		this(documentCriteria, customMessage, documentId, null, timestamp);
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
