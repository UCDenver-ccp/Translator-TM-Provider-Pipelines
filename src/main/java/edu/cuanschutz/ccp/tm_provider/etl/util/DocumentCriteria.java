package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.Serializable;

import lombok.Data;

@Data
public class DocumentCriteria implements Serializable {
	private static final long serialVersionUID = 1L;

	private final DocumentType documentType;
	private final DocumentFormat documentFormat;
	private final PipelineKey pipelineKey;
	private final String pipelineVersion;
}