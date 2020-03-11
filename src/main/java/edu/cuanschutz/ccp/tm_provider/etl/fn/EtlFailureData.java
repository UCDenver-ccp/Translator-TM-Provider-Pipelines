package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFn;

import lombok.Getter;

/**
 * Modified from:
 * https://github.com/vllry/beam-errorhandle-example/blob/master/src/main/java/errorfilteringdemo/datum/Failure.java
 */
@SuppressWarnings("rawtypes")
public class EtlFailureData extends DoFn {

	private static final long serialVersionUID = 1L;

	@Getter
	private String failedClass;
	@Getter
	private String message;
	@Getter
	private String fileId;
	@Getter
	private String stackTrace;

	public EtlFailureData(String customMessage, Object precursorData, Object datum, Throwable thrown) {
		this.failedClass = datum.getClass().toString();
		this.message = customMessage + " -- " + thrown.toString();
		this.fileId = precursorData.toString();
		this.stackTrace = Arrays.toString(thrown.getStackTrace());
	}

	@Override
	public String toString() {
		return "{" + "\nfailedClass: " + this.failedClass + "\nmessage: " + this.message + "\nfile ID: " + this.fileId
				+ "\nstackTrace: " + this.stackTrace + "\n}";
	}

}
