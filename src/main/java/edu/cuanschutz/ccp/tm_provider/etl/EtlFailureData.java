package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFn;

import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
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
	private final PipelineKey pipeline;
	private final String pipelineVersion;
	private final com.google.cloud.Timestamp timestamp;

	public EtlFailureData(PipelineKey pipeline, String pipelineVersion, String customMessage, String documentId,
			Throwable thrown, com.google.cloud.Timestamp timestamp) {
		this.pipeline = pipeline;
		this.pipelineVersion = pipelineVersion;
		this.message = customMessage + " -- " + thrown.toString();
		this.documentId = documentId;
		this.stackTrace = Arrays.toString(thrown.getStackTrace());
		this.timestamp = timestamp;
	}

}
