package edu.cuanschutz.ccp.tm_provider.etl.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Simple function that takes an input String and converts it to a KV with a
 * boolean. This is useful for combining key values from different maps, for
 * example the document ID resulting from all possible processing runs and those
 * resulting from failed processing runs.
 *
 */
public class ToKVFn extends DoFn<String, KV<String, Boolean>> {
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(@Element String docId, OutputReceiver<KV<String, Boolean>> out) {
		out.output(KV.of(docId, true));
	}

}