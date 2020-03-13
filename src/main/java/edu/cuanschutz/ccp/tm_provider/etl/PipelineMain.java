package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.Arrays;
import java.util.stream.Collectors;

import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

public class PipelineMain {
	public static void main(String[] args) {
		System.out.println("Running pipeline version: " + Version.getProjectVersion());
		PipelineKey pipeline = null;
		try {
			pipeline = PipelineKey.valueOf(args[0]);
		} catch (IllegalArgumentException e) {
			System.err.println(String.format("Unrecognized pipeline (%s). Valid choices are %s", args[0],
					Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			pipeline = null;
		}
		if (pipeline != null) {
			String[] pipelineArgs = Arrays.copyOfRange(args, 1, args.length);
			switch (pipeline) {
			case BIOC_TO_TEXT:
				BiocToTextPipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE:
				DependencyParsePipeline.main(pipelineArgs);
				break;

			default:
				throw new IllegalArgumentException(String.format(
						"Valid pipeline (%s) but a code change required before it can be used. Valid choices are %s",
						args[0],
						Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			}
		}
	}
}
