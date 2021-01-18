package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NGDProcessCountsPipelineTest {

	/**
	 * Test values come from Shakespeare example from Wikipedia: https://en.wikipedia.org/wiki/Normalized_Google_distance
	 */
	@Test
	public void testNormalizedGoogleDistance() {

		long fx = 130000000l;
		long fy = 26000000l;
		long fxy = 20800000l;
		long N = 25270000000000l;

		double ngd = NGDKgxPipeline.normalizedGoogleDistance(N, fx, fy, fxy);

		assertEquals(0.13, ngd, 0.005);
	}

}
